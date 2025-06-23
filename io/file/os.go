package file

import (
	"io/fs"
	"os"
)

func createNewWriteOnlyFile(path string, mode os.FileMode, overwrite bool) (*os.File, error) {
	flags := os.O_CREATE | os.O_EXCL | os.O_WRONLY | os.O_SYNC
	if overwrite {
		flags |= os.O_TRUNC
	}
	f, err := os.OpenFile(path, flags, mode)
	if os.IsExist(err) {
		return nil, ErrExisting
	}
	return f, err
}

func mayOpenReadOnlyFile(path string) (*os.File, fs.FileMode, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if os.IsNotExist(err) {
		return nil, 0644, nil
	}
	if err != nil {
		return nil, 0644, err
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, 0644, err
	}
	return f, stat.Mode(), nil
}
