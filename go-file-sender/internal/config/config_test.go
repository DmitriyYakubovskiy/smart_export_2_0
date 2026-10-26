package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadValidConfig(t *testing.T) {
    path := filepath.Join(t.TempDir(), "config.json")
    data := []byte(`{"files_dir":"files","service_url":"http://localhost:8000/upload"}`)

    if err := os.WriteFile(path, data, 0600); err != nil {
        t.Fatal(err)
    }

    cfg, err := Load(path)
    if err != nil {
        t.Fatal(err)
    }

    if cfg.FilesDir != "files" {
        t.Errorf("files_dir: got %q, want files", cfg.FilesDir)
    }
    if cfg.ServiceURL != "http://localhost:8000/upload" {
        t.Errorf("Unexpected service_url: %q", cfg.ServiceURL)
    }
}

func TestLoadMissingConfig(t *testing.T) {
    _, err := Load(filepath.Join(t.TempDir(), "missing.json"))
    if err == nil {
        t.Fatal("Expected error for missing config")
    }
}

func TestLoadInvalidJSON(t *testing.T) {
    path := filepath.Join(t.TempDir(), "config.json")
    if err := os.WriteFile(path, []byte(`{"files_dir":`), 0600); err != nil {
        t.Fatal(err)
    }

    _, err := Load(path)
    if err == nil {
        t.Fatal("Expected JSON parsing error")
    }
}
