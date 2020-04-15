package validation

import (
	"strings"

	"github.com/syncromatics/kafmesh/internal/models"

	"github.com/pkg/errors"
)

// ValidateService validates the service definition is valid
func ValidateService(service *models.Service) error {
	if strings.Trim(service.Name, " ") == "" {
		return errors.Errorf("service must have a name")
	}

	if strings.Trim(service.Description, " ") == "" {
		return errors.Errorf("service must have a description")
	}

	if service.Defaults.Partition <= 0 {
		return errors.Errorf("service must not be <= 0 for default partition size")
	}

	if service.Defaults.Replication <= 0 {
		return errors.Errorf("service must not be <= 0 for default replication size")
	}

	if strings.Trim(service.Output.Package, " ") == "" {
		return errors.Errorf("service must have an output package name")
	}

	if strings.Trim(service.Output.Package, " ") == "" {
		return errors.Errorf("service must have an output path")
	}

	if strings.HasPrefix(service.Output.Path, "/") {
		return errors.Errorf("service output path must be relative")
	}

	return nil
}
