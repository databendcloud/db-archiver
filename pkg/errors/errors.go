package errors

import "github.com/pkg/errors"

type SyncTaskError struct {
	Kind       string
	StatusCode int
	Message    string
}

func (err *SyncTaskError) Error() string {
	return err.Message
}

var (
	TaskNotFound = SyncTaskError{"TaskNotFound", 404, "Sync task not found"}
	TaskRunning  = SyncTaskError{"TaskRunning", 404, "Sync task is running"}
)

func Is(err error, target error) bool {
	return errors.Is(err, target)
}

func As(err error, target interface{}) bool {
	return errors.As(err, target)
}

func AsCloudControlError(err error, gwErr **SyncTaskError) bool {
	return errors.As(err, gwErr)
}

func Wrapf(err error, format string, args ...interface{}) error {
	return errors.Wrapf(err, format, args...)
}
