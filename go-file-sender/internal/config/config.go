package config

import (
	"encoding/json"
	"os"
)

type Config struct {
	FilesDir string `json:"files_dir"`
	ServiceURL string  `json:"service_url"`
}

func Load(path string) (Config, error) {
	var cfg Config

	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}

	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}
	
	return cfg, nil
}

