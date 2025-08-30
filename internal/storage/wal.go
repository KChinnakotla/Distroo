package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

type WAL struct {
	path string
	f    *os.File
}

func OpenWAL(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "wal.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &WAL{path: path, f: f}, nil
}

func (w *WAL) Append(b []byte) error {
	if w == nil || w.f == nil {
		return fmt.Errorf("wal not open")
	}
	_, err := w.f.Write(append(b, '\n'))
	return err
}

func (w *WAL) Close() error {
	if w == nil || w.f == nil {
		return nil
	}
	return w.f.Close()
}
