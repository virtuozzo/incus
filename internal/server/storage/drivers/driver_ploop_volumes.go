package drivers

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"runtime"
	"strings"

	"bitbucket.org/aleskinprivate/vzgoploop"
	"github.com/lxc/incus/v6/internal/instancewriter"
	"github.com/lxc/incus/v6/internal/rsync"
	"github.com/lxc/incus/v6/internal/server/backup"
	"github.com/lxc/incus/v6/internal/server/migration"
	"github.com/lxc/incus/v6/internal/server/operations"
	"github.com/lxc/incus/v6/shared/api"
	"github.com/lxc/incus/v6/shared/logger"
	"github.com/lxc/incus/v6/shared/revert"
	"github.com/lxc/incus/v6/shared/units"
	"github.com/lxc/incus/v6/shared/util"
)

const defaultPloopSize = 10 * 1024 * 1024 //10Gb
const defaultFileName = "root.hds"
const defaultDescriptor = "DiskDescriptor.xml"
const snapshotInfo = "snapshot.info"
const maxTraceDepth = 5

func (d *ploop) PrintTrace(info string, depth int) {

	if depth > maxTraceDepth {
		depth = maxTraceDepth
	}

	if info != "" && depth > 1 {
		d.logger.Debug("VZ Ploop: Trace", logger.Ctx{"info": info, "depth": depth})
	}

	for i := 0; i < depth; i++ {
		pc, _, _, _ := runtime.Caller(depth - i)
		d.logger.Debug("VZ Ploop: Trace", logger.Ctx{"frame": depth - i, "func": strings.Replace(runtime.FuncForPC(pc).Name(),
			"github.com/lxc/incus/v6/internal/server/storage", "", 1)})
	}
}

// CreateVolume creates an empty volume and can optionally fill it by executing the supplied
// filler function.
func (d *ploop) CreateVolume(vol Volume, filler *VolumeFiller, op *operations.Operation) error {

	d.PrintTrace("Create", 3)
	volPath := vol.MountPath()
	d.logger.Debug("VZ Ploop: Create Volume", logger.Ctx{"MountPath": volPath, "Name": vol.name, "Type": vol.volType})

	d.logger.Debug("VZ Ploop: Create Volume 2", logger.Ctx{"ContenType": vol.contentType})

	if vol.volType == VolumeTypeBucket {
		return fmt.Errorf("VZ Ploop: Unsupported Volume Type %s", vol.volType)
	}

	revert := revert.New()
	defer revert.Fail()

	if util.PathExists(vol.MountPath()) {
		return fmt.Errorf("VZ Ploop: Volume path %q already exists", vol.MountPath())
	}

	// Create the volume itself.
	err := vol.EnsureMountPath()
	if err != nil {
		return err
	}
	revert.Add(func() { _ = os.RemoveAll(volPath) })

	// Get path to disk volume if volume is block or iso.
	rootBlockPath := ""
	if IsContentBlock(vol.contentType) {
		// We expect the filler to copy the VM image into this path.
		rootBlockPath, err = d.GetVolumeDiskPath(vol)
		if err != nil {
			return err
		}
	}

	if vol.volType == VolumeTypeContainer {
		//create ploop device
		param := vzgoploop.VZP_CreateParam{
			Size:  defaultPloopSize,
			Image: volPath + "/" + defaultFileName,
		}

		res := vzgoploop.Create(&param)

		if res.Status != vzgoploop.VZP_SUCCESS {
			return fmt.Errorf("VZ Ploop: Can't create disk: %s \n", res.Msg)
		}

		disk, res := vzgoploop.Open(volPath + "/" + defaultDescriptor)
		if res.Status != vzgoploop.VZP_SUCCESS {
			return fmt.Errorf("VZ Ploop: Can't open disk: %s \n", res.Msg)
		}

		mp := vzgoploop.VZP_MountParam{Target: volPath + "/rootfs"}

		_ = os.Mkdir(mp.Target, 0755) //TODO
		device, res := disk.MountImage(&mp)
		if res.Status != vzgoploop.VZP_SUCCESS {
			return fmt.Errorf("VZ Ploop: Can't mount image create: %s \n", res.Msg)
		}

		d.logger.Debug("VZ Ploop: Mounted", logger.Ctx{"device": device})

		// Run the volume filler function if supplied.
		err = d.runFiller(vol, rootBlockPath, filler, false)
		if err != nil {
			return err
		}

		res = disk.UmountImage()
		if res.Status != vzgoploop.VZP_SUCCESS {
			return fmt.Errorf("VZ Ploop: Can't umount image: %s \n", res.Msg)
		}

		disk.Close()
	} else {
		// Run the volume filler function if supplied.
		err = d.runFiller(vol, rootBlockPath, filler, false)
		if err != nil {
			return err
		}
	}

	// If we are creating a block volume, resize it to the requested size or the default.
	// For block volumes, we expect the filler function to have converted the qcow2 image to raw into the rootBlockPath.
	// For ISOs the content will just be copied.
	if IsContentBlock(vol.contentType) {
		// Convert to bytes.
		sizeBytes, err := units.ParseByteSizeString(vol.ConfigSize())
		if err != nil {
			return err
		}

		// Ignore ErrCannotBeShrunk when setting size this just means the filler run above has needed to
		// increase the volume size beyond the default block volume size.
		_, err = ensureVolumeBlockFile(vol, rootBlockPath, sizeBytes, false)
		if err != nil && !errors.Is(err, ErrCannotBeShrunk) {
			return err
		}

		// Move the GPT alt header to end of disk if needed and if filler specified.
		if vol.IsVMBlock() && filler != nil && filler.Fill != nil {
			err = d.moveGPTAltHeader(rootBlockPath)
			if err != nil {
				return err
			}
		}
	}

	d.logger.Debug("VZ Ploop: Created Volume successfully\n")
	revert.Success()
	return nil
}

// DeleteVolume deletes a volume of the storage device. If any snapshots of the volume remain then
// this function will return an error.
func (d *ploop) DeleteVolume(vol Volume, op *operations.Operation) error {

	if vol.volType == VolumeTypeBucket {
		return fmt.Errorf("VZ Ploop: Unsupported Volume Type %s", vol.volType)
	}

	snapshots, err := d.VolumeSnapshots(vol, op)
	if err != nil {
		return err
	}

	if len(snapshots) > 0 {
		return fmt.Errorf("Cannot remove a volume that has snapshots")
	}

	volPath := vol.MountPath()

	// If the volume doesn't exist, then nothing more to do.
	if !util.PathExists(volPath) {
		return nil
	}

	// Remove the volume from the storage device.
	err = forceRemoveAll(volPath)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("Failed to remove '%s': %w", volPath, err)
	}

	// Although the volume snapshot directory should already be removed, lets remove it here
	// to just in case the top-level directory is left.
	err = deleteParentSnapshotDirIfEmpty(d.name, vol.volType, vol.name)
	if err != nil {
		return err
	}

	return nil
}

// HasVolume indicates whether a specific volume exists on the storage pool.
func (d *ploop) HasVolume(vol Volume) (bool, error) {
	d.PrintTrace("HasVolume", 3)
	return genericVFSHasVolume(vol)
}

// FillVolumeConfig populate volume with default config.
func (d *ploop) FillVolumeConfig(vol Volume) error {
	d.PrintTrace("", 1)

	initialSize := vol.config["size"]

	err := d.fillVolumeConfig(&vol)
	if err != nil {
		return err
	}

	// Buckets do not support default volume size.
	// If size is specified manually, do not remove, so it triggers validation failure and an error to user.
	if vol.volType == VolumeTypeBucket && initialSize == "" {
		delete(vol.config, "size")
	}

	return nil
}

// ValidateVolume validates the supplied volume config. Optionally removes invalid keys from the volume's config.
func (d *ploop) ValidateVolume(vol Volume, removeUnknownKeys bool) error {

	d.PrintTrace(vol.name, 1)

	if vol.volType != VolumeTypeContainer {
		err := d.validateVolume(vol, nil, removeUnknownKeys)
		if err != nil {
			d.logger.Debug("VZ Ploop: Size cannot be specified for non-containers type")
			return err
		}

		if vol.config["size"] != "" && vol.volType == VolumeTypeBucket {
			return fmt.Errorf("VZ Ploop: Size cannot be specified for buckets")
		}
	}

	return nil
}

// CreateVolumeFromBackup restores a backup tarball onto the storage device.
func (d *ploop) CreateVolumeFromBackup(vol Volume, srcBackup backup.Info, srcData io.ReadSeeker, op *operations.Operation) (VolumePostHook, revert.Hook, error) {
	d.PrintTrace("", 1)

	return nil, nil, nil
}

// CreateVolumeFromCopy provides same-pool volume copying functionality.
func (d *ploop) CreateVolumeFromCopy(vol Volume, srcVol Volume, copySnapshots bool, allowInconsistent bool, op *operations.Operation) error {
	d.PrintTrace("", 1)

	return nil
}

// CreateVolumeFromMigration creates a volume being sent via a migration.
func (d *ploop) CreateVolumeFromMigration(vol Volume, conn io.ReadWriteCloser, volTargetArgs migration.VolumeTargetArgs, preFiller *VolumeFiller, op *operations.Operation) error {
	d.PrintTrace("", 1)

	return nil
}

// RefreshVolume provides same-pool volume and specific snapshots syncing functionality.
func (d *ploop) RefreshVolume(vol Volume, srcVol Volume, srcSnapshots []Volume, allowInconsistent bool, op *operations.Operation) error {
	d.PrintTrace("", 1)

	return nil
}

// UpdateVolume applies config changes to the volume.
func (d *ploop) UpdateVolume(vol Volume, changedConfig map[string]string) error {
	d.PrintTrace("UpdateVolume", 2)

	i := 1
	for key, val := range changedConfig {
		d.logger.Debug("VZ Ploop: Change config for", logger.Ctx{"item": i, "key": key, "value": val})
		i++
	}

	if vol.volType == VolumeTypeContainer {
		val, ok := changedConfig["size"]
		if ok {

			disk, res := vzgoploop.Open(vol.MountPath() + "/" + defaultDescriptor)
			if res.Status != vzgoploop.VZP_SUCCESS {
				return fmt.Errorf("VZ Ploop: Can't open disk: %s \n", res.Msg)
			}

			defer disk.Close()

			newSize, err := units.ParseByteSizeString(val)
			if err != nil {
				return err
			}

			err = d.SetVolumeQuota(vol, val, false, nil)
			if err != nil {
				return err
			}

			ploopSize := uint64(newSize / 1024)
			d.logger.Debug("VZ Ploop: Update volume ", logger.Ctx{"newSize": ploopSize})
			res = disk.Resize(ploopSize, false)
			if res.Status != vzgoploop.VZP_SUCCESS {
				return fmt.Errorf("VZ Ploop: Can't change disk size: %s \n", res.Msg)
			}
			d.logger.Debug("VZ Ploop: Disk updated ")
		}

		return nil
	}

	if vol.contentType != ContentTypeFS {
		return ErrNotSupported
	}

	_, changed := changedConfig["size"]
	if changed {
		err := d.SetVolumeQuota(vol, changedConfig["size"], false, nil)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetVolumeUsage returns the disk space used by the volume.
func (d *ploop) GetVolumeUsage(vol Volume) (int64, error) {
	d.PrintTrace("Usage for:"+vol.MountPath()+"/"+defaultDescriptor, 3)

	// Snapshot usage not supported for Ploop.
	if vol.IsSnapshot() {
		return -1, ErrNotSupported
	}

	if vol.volType == VolumeTypeContainer {
		stats, res := vzgoploop.GetDiskStats(vol.MountPath() + "/" + defaultDescriptor)
		if res.Status != vzgoploop.VZP_SUCCESS {
			return -1, fmt.Errorf("VZ Ploop: Can't get disk stats: %s \n", res.Msg)
		}
		return int64(stats.TotalSize), nil
	}
	return 0, nil
}

// SetVolumeQuota applies a size limit on volume.
func (d *ploop) SetVolumeQuota(vol Volume, size string, allowUnsafeResize bool, op *operations.Operation) error {
	d.PrintTrace("", 1)

	return nil
}

// GetVolumeDiskPath returns the location of a disk volume.
func (d *ploop) GetVolumeDiskPath(vol Volume) (string, error) {
	d.PrintTrace("", 1)
	return genericVFSGetVolumeDiskPath(vol)
}

// ListVolumes returns a list of volumes in storage pool.
func (d *ploop) ListVolumes() ([]Volume, error) {
	d.PrintTrace("VZ Ploop: ListVolumes", 2)
	return genericVFSListVolumes(d)
}

// MountVolume simulates mounting a volume.
func (d *ploop) MountVolume(vol Volume, op *operations.Operation) error {

	d.PrintTrace(": "+vol.name+"; ["+vol.MountPath()+"]", 3)

	unlock, err := vol.MountLock()
	if err != nil {
		return err
	}

	defer unlock()

	// Don't attempt to modify the permission of an existing custom volume root.
	// A user inside the instance may have modified this and we don't want to reset it on restart.
	if !util.PathExists(vol.MountPath()) || vol.volType != VolumeTypeCustom {
		err := vol.EnsureMountPath()
		if err != nil {
			return err
		}
	}

	if vol.volType == VolumeTypeContainer {
		disk, res := vzgoploop.Open(vol.MountPath() + "/" + defaultDescriptor)
		if res.Status != vzgoploop.VZP_SUCCESS {
			return fmt.Errorf("VZ Ploop: Can't open disk: %s \n", res.Msg)
		}

		defer disk.Close()

		status, res := disk.IsMounted()
		if res.Status != vzgoploop.VZP_SUCCESS {
			d.logger.Warn("VZ Ploop: Can't get mount disk status after mount", logger.Ctx{"msg": res.Msg})
			return nil
		}

		if status {
			count := vol.MountRefCountIncrement()
			d.logger.Debug("VZ Ploop: MountVolume - already mounted", logger.Ctx{"counter": count})
			return nil

		}

		mp := vzgoploop.VZP_MountParam{Target: vol.MountPath() + "/rootfs"}

		device, res := disk.MountImage(&mp)
		if res.Status != vzgoploop.VZP_SUCCESS {
			d.logger.Warn("VZ Ploop: Can't mount image Mount", logger.Ctx{"msg": res.Msg})
			return nil //TODO already mounted check
		}

		d.logger.Debug("VZ Ploop: MountVolume - Done", logger.Ctx{"device": device})
	}

	count := vol.MountRefCountIncrement() // From here on it is up to caller to call UnmountVolume() when done.
	d.logger.Debug("VZ Ploop: MountVolume", logger.Ctx{"counter": count})

	return nil
}

// UnmountVolume simulates unmounting a volume. As dir driver doesn't have volumes to unmount it
// returns false indicating the volume was already unmounted.
func (d *ploop) UnmountVolume(vol Volume, keepBlockDev bool, op *operations.Operation) (bool, error) {

	d.PrintTrace(": "+vol.name+"; ["+vol.MountPath()+"]", 3)

	unlock, err := vol.MountLock()
	if err != nil {
		return false, err
	}

	defer unlock()

	refCount := vol.MountRefCountDecrement()
	if refCount > 0 {
		d.logger.Info("VZ Ploop: Skipping unmount as in use", logger.Ctx{"volName": vol.name, "refCount": refCount})
		return false, ErrInUse
	}

	if vol.volType == VolumeTypeContainer {
		disk, res := vzgoploop.Open(vol.MountPath() + "/" + defaultDescriptor)

		if res.Status != vzgoploop.VZP_SUCCESS {
			d.logger.Error("VZ Ploop: Can't open disk", logger.Ctx{"msg": res.Msg})
		}

		defer disk.Close()

		res = disk.UmountImage()
		if res.Status != vzgoploop.VZP_SUCCESS {
			d.logger.Error("VZ Ploop: Can't umount image", logger.Ctx{"msg": res.Msg})
		}

		status, res := disk.IsMounted()
		if res.Status != vzgoploop.VZP_SUCCESS {
			d.logger.Warn("VZ Ploop: Can't get mount disk status after umount", logger.Ctx{"msg": res.Msg})
		}

		if status {
			d.logger.Error("VZ Ploop: Disk is unexpected mounted", logger.Ctx{"volume": vol.name})
		}
	}

	return false, nil
}

// RenameVolume renames a volume and its snapshots.
func (d *ploop) RenameVolume(vol Volume, newVolName string, op *operations.Operation) error {
	d.PrintTrace("", 1)
	return genericVFSRenameVolume(d, vol, newVolName, op)
}

// MigrateVolume sends a volume for migration.
func (d *ploop) MigrateVolume(vol Volume, conn io.ReadWriteCloser, volSrcArgs *migration.VolumeSourceArgs, op *operations.Operation) error {
	d.PrintTrace("", 1)
	return genericVFSMigrateVolume(d, d.state, vol, conn, volSrcArgs, op)
}

// BackupVolume copies a volume (and optionally its snapshots) to a specified target path.
// This driver does not support optimized backups.
func (d *ploop) BackupVolume(vol Volume, tarWriter *instancewriter.InstanceTarWriter, optimized bool, snapshots []string, op *operations.Operation) error {
	d.PrintTrace("", 1)
	return genericVFSBackupVolume(d, vol, tarWriter, snapshots, op)
}

// CreateVolumeSnapshot creates a snapshot of a volume.
func (d *ploop) CreateVolumeSnapshot(snapVol Volume, op *operations.Operation) error {
	d.PrintTrace("Create for:"+snapVol.MountPath(), 3)

	parentName, snapName, _ := api.GetParentAndSnapshotName(snapVol.name)
	//srcDevPath, err := d.GetVolumeDiskPath(parentVol)
	srcPath := GetVolumeMountPath(d.name, snapVol.volType, parentName)

	d.logger.Debug("VZ Ploop: Create snapshot",
		logger.Ctx{"type": snapVol.volType, "parent": parentName, "snapName": snapName, "srcPath": srcPath})

	// Create snapshot directory.
	err := snapVol.EnsureMountPath()
	if err != nil {
		return err
	}

	revert := revert.New()
	defer revert.Fail()

	snapPath := snapVol.MountPath()
	revert.Add(func() { _ = os.RemoveAll(snapPath) })

	if snapVol.volType == VolumeTypeContainer {

		var rsyncArgs []string

		rsyncArgs = append(rsyncArgs, "--exclude", defaultFileName)
		rsyncArgs = append(rsyncArgs, "--exclude", defaultDescriptor)
		rsyncArgs = append(rsyncArgs, "--exclude", ".statfs")
		rsyncArgs = append(rsyncArgs, "--exclude", defaultFileName+"*")
		rsyncArgs = append(rsyncArgs, "--exclude", "rootfs")

		bwlimit := d.config["rsync.bwlimit"]
		srcPath := GetVolumeMountPath(d.name, snapVol.volType, parentName)
		d.Logger().Debug("VZ Ploop: Copying fileystem volume", logger.Ctx{"sourcePath": srcPath, "targetPath": snapPath, "bwlimit": bwlimit, "rsyncArgs": rsyncArgs})

		// Copy filesystem volume into snapshot directory.
		_, err = rsync.LocalCopy(srcPath, snapPath, bwlimit, true, rsyncArgs...)
		if err != nil {
			return err
		}

		disk, res := vzgoploop.Open(srcPath + "/" + defaultDescriptor)

		if res.Status != vzgoploop.VZP_SUCCESS {
			return fmt.Errorf("VZ Ploop: Can't open disk: %s \n", res.Msg)
		}
		defer disk.Close()

		//create snapshot
		uuid, res := disk.CreateSnapshot("")
		//TODO: - it seems parameter inside CreateSnapshot() does not work or ignores, or has another meaning
		//need additional investigation. I assumed to use here snapVol.MountPath()

		if res.Status != vzgoploop.VZP_SUCCESS {
			return fmt.Errorf("VZ Ploop: Can't create snapshot: %s \n", res.Msg)
		}
		d.logger.Debug("VZ Ploop: Created snapshot", logger.Ctx{"uuid": uuid})

		//todo - think about revert
		fSnapshotInfo, err := os.Create(snapPath + "/" + snapshotInfo)
		if err != nil {
			return err
		}

		defer fSnapshotInfo.Close()

		_, err = fSnapshotInfo.WriteString("uuid = " + uuid + "\npath = " + srcPath)
		if err != nil {
			return err
		}
		fSnapshotInfo.Sync()
	}

	revert.Success()
	return nil
}

// DeleteVolumeSnapshot removes a snapshot from the storage device. The volName and snapshotName
// must be bare names and should not be in the format "volume/snapshot".
func (d *ploop) DeleteVolumeSnapshot(snapVol Volume, op *operations.Operation) error {
	d.PrintTrace("", 1)
	snapPath := snapVol.MountPath()

	if snapVol.volType == VolumeTypeContainer {
		b, err := os.ReadFile(snapPath + "/" + snapshotInfo)
		if err != nil {
			fmt.Print(err)
		}

		info := strings.Split(string(b), "\n")

		var uuid, srcPath string
		for _, line := range info {
			key, val, _ := strings.Cut(line, " = ")
			if key == "uuid" {
				uuid = val
			} else if key == "path" {
				srcPath = val
			}
		}

		d.logger.Debug("VZ Ploop: Remove Snapshot", logger.Ctx{"uuid": uuid, "srcPath": srcPath})

		if srcPath == "" || uuid == "" {
			return fmt.Errorf("Failed to remove. Wrong parameters in the config file: '%s' [ uuid =%s; path = %s]",
				snapPath+"/"+snapshotInfo, uuid, srcPath)
		}

		disk, res := vzgoploop.Open(srcPath + "/" + defaultDescriptor)

		if res.Status != vzgoploop.VZP_SUCCESS {
			d.logger.Error("VZ Ploop: Can't open disk", logger.Ctx{"msg": res.Msg})
		} else {
			res = disk.DeleteSnapshot(uuid)
			if res.Status != vzgoploop.VZP_SUCCESS {
				return fmt.Errorf("VZ Ploop: Can't delete snapshot: %s", res.Msg)
			}
			disk.Close()
		}
	}

	// Remove the snapshot from the storage device.
	err := forceRemoveAll(snapPath)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("Failed to remove '%s': %w", snapPath, err)
	}

	parentName, _, _ := api.GetParentAndSnapshotName(snapVol.name)

	// Remove the parent snapshot directory if this is the last snapshot being removed.
	err = deleteParentSnapshotDirIfEmpty(d.name, snapVol.volType, parentName)
	if err != nil {
		return err
	}
	return nil
}

// MountVolumeSnapshot sets up a read-only mount on top of the snapshot to avoid accidental modifications.
func (d *ploop) MountVolumeSnapshot(snapVol Volume, op *operations.Operation) error {
	d.PrintTrace("", 1)

	//TODO: need investigation for purposes of this methods
	//I do not see reason to implement this for now.
	//Ploop driver has the function 'ploop_mount_snapshot' but it does not use
	//So, for this moment we will keep default mock stub for MountVolumeSnapshot/UnmountVolumeSnapshot

	return nil
}

// UnmountVolumeSnapshot removes the read-only mount placed on top of a snapshot.
func (d *ploop) UnmountVolumeSnapshot(snapVol Volume, op *operations.Operation) (bool, error) {
	d.PrintTrace("", 1)

	return true, nil
}

// VolumeSnapshots returns a list of snapshots for the volume (in no particular order).
func (d *ploop) VolumeSnapshots(vol Volume, op *operations.Operation) ([]string, error) {
	d.PrintTrace("List of Snapshots for: "+vol.MountPath()+"/"+defaultDescriptor, 1)
	return genericVFSVolumeSnapshots(d, vol, op)
}

// RestoreVolume restores a volume from a snapshot.
func (d *ploop) RestoreVolume(vol Volume, snapshotName string, op *operations.Operation) error {
	d.PrintTrace("", 1)

	snapPath := vol.MountPath()

	snapVol, err := vol.NewSnapshot(snapshotName)
	if err != nil {
		return err
	}
	srcPath := snapVol.MountPath()

	if vol.volType == VolumeTypeContainer {
		b, err := os.ReadFile(srcPath + "/" + snapshotInfo)
		if err != nil {
			fmt.Print(err)
		}

		info := strings.Split(string(b), "\n")

		var uuid string
		for _, line := range info {
			key, val, _ := strings.Cut(line, " = ")
			if key == "uuid" {
				uuid = val
				break
			}
		}

		d.logger.Debug("VZ Ploop: Switch Snapshot", logger.Ctx{"uuid": uuid, "path": snapPath})

		if uuid == "" {
			return fmt.Errorf("Failed to switch on snapshot. Wrong parameters in the config file: '%s' [ uuid =%s]",
				snapPath+"/"+snapshotInfo, uuid)
		}

		disk, res := vzgoploop.Open(snapPath + "/" + defaultDescriptor)
		defer disk.Close()

		if res.Status != vzgoploop.VZP_SUCCESS {
			d.logger.Error("VZ Ploop: Can't open disk", logger.Ctx{"msg": res.Msg})
		}

		res = disk.SwitchSnapshot(uuid)
		if res.Status != vzgoploop.VZP_SUCCESS {
			return fmt.Errorf("VZ Ploop: Can't switch to snapshot: %s", res.Msg)
		}

		var rsyncArgs []string

		rsyncArgs = append(rsyncArgs, "--exclude", snapshotInfo)
		rsyncArgs = append(rsyncArgs, "--exclude", defaultFileName)
		rsyncArgs = append(rsyncArgs, "--exclude", defaultDescriptor)
		rsyncArgs = append(rsyncArgs, "--exclude", ".statfs")
		rsyncArgs = append(rsyncArgs, "--exclude", defaultFileName+"*")
		rsyncArgs = append(rsyncArgs, "--exclude", "rootfs")

		bwlimit := d.config["rsync.bwlimit"]
		d.Logger().Debug("VZ Ploop: Copying fileystem volume", logger.Ctx{"sourcePath": srcPath, "targetPath": snapPath, "bwlimit": bwlimit, "rsyncArgs": rsyncArgs})

		// Copy filesystem volume into snapshot directory.
		_, err = rsync.LocalCopy(srcPath, snapPath, bwlimit, true, rsyncArgs...)
		if err != nil {
			return err
		}
	}

	return nil
}

// RenameVolumeSnapshot renames a volume snapshot.
func (d *ploop) RenameVolumeSnapshot(snapVol Volume, newSnapshotName string, op *operations.Operation) error {
	d.PrintTrace("", 1)

	return genericVFSRenameVolumeSnapshot(d, snapVol, newSnapshotName, op)
}
