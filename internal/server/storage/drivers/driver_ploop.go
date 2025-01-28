package drivers

import (
	"fmt"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"

	"bitbucket.org/aleskinprivate/vzgoploop"
	deviceConfig "github.com/lxc/incus/v6/internal/server/device/config"
	"github.com/lxc/incus/v6/internal/server/operations"
	"github.com/lxc/incus/v6/internal/server/state"
	internalUtil "github.com/lxc/incus/v6/internal/util"
	"github.com/lxc/incus/v6/shared/api"
	"github.com/lxc/incus/v6/shared/logger"
	"github.com/lxc/incus/v6/shared/util"
)

type ploop struct {
	common
}

// load is used to run one-time action per-driver rather than per-pool.
func (d *ploop) load() error {
	// Register the patches.
	d.patches = map[string]func() error{
		"storage_lvm_skipactivation":                         nil,
		"storage_missing_snapshot_records":                   nil,
		"storage_delete_old_snapshot_records":                nil,
		"storage_zfs_drop_block_volume_filesystem_extension": nil,
		"storage_prefix_bucket_names_with_project":           nil,
	}

	return nil
}

// Info returns info about the driver and its environment.
func (d *ploop) Info() Info {
	return Info{
		Name:                         "ploop",
		Version:                      "1",
		DefaultVMBlockFilesystemSize: deviceConfig.DefaultVMBlockFilesystemSize,
		OptimizedImages:              false,
		PreservesInodes:              false,
		Remote:                       d.isRemote(),
		VolumeTypes:                  []VolumeType{VolumeTypeBucket, VolumeTypeCustom, VolumeTypeImage, VolumeTypeContainer, VolumeTypeVM},
		VolumeMultiNode:              d.isRemote(),
		BlockBacking:                 false,
		RunningCopyFreeze:            true,
		DirectIO:                     true,
		IOUring:                      true,
		MountedRoot:                  true,
		Buckets:                      true,
	}
}

func (d *ploop) init(s *state.State, name string, config map[string]string, log logger.Logger, volIDFunc func(volType VolumeType, volName string) (int64, error), commonRules *Validators) {

	// Setup logger VZ Ploop
	err := logger.InitLogger("/var/log/incus/vzploop.log", "", true, true, nil)
	if err != nil {
		fmt.Printf("VZ Ploop: Cannot initiate Ploop Logger. Working with default parameters\n")
	}
	d.common.init(s, name, config, log, volIDFunc, commonRules)

	//TODO: investigate logging of incus and split information about ploop and other
	//TODO: Fix global parameters for logfile
}

// FillConfig populates the storage pool's configuration file with the default values.
func (d *ploop) FillConfig() error {
	// Set default source if missing.
	if d.config["source"] == "" {
		d.config["source"] = GetPoolMountPath(d.name)
	}

	return nil
}

// Create is called during pool creation and is effectively using an empty driver struct.
// WARNING: The Create() function cannot rely on any of the struct attributes being set.
func (d *ploop) Create() error {
	err := d.FillConfig()
	if err != nil {
		return err
	}

	sourcePath := d.config["source"]

	if !util.PathExists(sourcePath) {
		return fmt.Errorf("Source path '%s' doesn't exist", sourcePath)
	}

	d.logger.Info("VZ Ploop: Create ploop storage.", logger.Ctx{"about": vzgoploop.About()})

	// Check that if within INCUS_DIR, we're at our expected spot.
	cleanSource := filepath.Clean(sourcePath)
	varPath := strings.TrimRight(internalUtil.VarPath(), "/") + "/"
	if (cleanSource == internalUtil.VarPath() || strings.HasPrefix(cleanSource, varPath)) && cleanSource != GetPoolMountPath(d.name) {
		return fmt.Errorf("Source path '%s' is within the Incus directory", cleanSource)
	}

	// Check that the path is currently empty.
	isEmpty, err := internalUtil.PathIsEmpty(sourcePath)
	if err != nil {
		return err
	}

	if !isEmpty {
		return fmt.Errorf("Source path '%s' isn't empty", sourcePath)
	}
	d.logger.Info("VZ Ploop: Created ploop storage")
	return nil
}

// Delete removes the storage pool from the storage device.
func (d *ploop) Delete(op *operations.Operation) error {
	// On delete, wipe everything in the directory.
	err := wipeDirectory(GetPoolMountPath(d.name))
	if err != nil {
		return err
	}

	// Unmount the path.
	_, err = d.Unmount()
	if err != nil {
		return err
	}
	return nil
}

// Validate checks that all provide keys are supported and that no conflicting or missing configuration is present.
func (d *ploop) Validate(config map[string]string) error {
	return d.validatePool(config, nil, nil)
}

// Update applies any driver changes required from a configuration change.
func (d *ploop) Update(changedConfig map[string]string) error {
	return nil
}

// Mount mounts the storage pool.
func (d *ploop) Mount() (bool, error) {
	path := GetPoolMountPath(d.name)
	sourcePath := d.config["source"]

	// Check if we're dealing with an external mount.
	if sourcePath == path {
		return false, nil
	}

	// Check if already mounted.
	if sameMount(sourcePath, path) {
		return false, nil
	}

	// Setup the bind-mount.
	err := TryMount(sourcePath, path, "none", unix.MS_BIND, "")
	if err != nil {
		return false, err
	}

	return true, nil
}

// Unmount unmounts the storage pool.
func (d *ploop) Unmount() (bool, error) {
	path := GetPoolMountPath(d.name)

	// Check if we're dealing with an external mount.
	if d.config["source"] == path {
		return false, nil
	}

	// Unmount until nothing is left mounted.
	return forceUnmount(path)
}

// GetResources returns the pool resource usage information.
func (d *ploop) GetResources() (*api.ResourcesStoragePool, error) {
	return genericVFSGetResources(d)
}
