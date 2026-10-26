package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go-file-sender/internal/config"
)

func main() {
	cfg, err := config.Load("config.json")
	if err != nil {
			log.Fatal(err)
	}

	fmt.Printf("Конфигурация: %+v\n", cfg)

	if cfg.FilesDir == "" {
			log.Fatal("Не указана папка files_dir в config.json")
	}

	if cfg.ServiceURL == "" {
			log.Fatal("Не указан service_url в config.json")
	}

	files, err := os.ReadDir(cfg.FilesDir)
	if err != nil {
		log.Fatal(err)
	}

	client := &http.Client{Timeout: 120 * time.Second}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		ext := strings.ToLower(filepath.Ext(file.Name()))
		if ext != ".png" && ext != ".pdf" {
			continue
		}

		path := filepath.Join(cfg.FilesDir, file.Name())

		fmt.Println("Отправка ",file.Name())

		err := sendFile(client, cfg.ServiceURL, path)
		
		if err != nil {
			log.Printf("Ошибка %s: %v", file.Name(), err)
			continue
		}

		fmt.Println("Успешно: ", file.Name())
	}
}

func sendFile(client *http.Client, url, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	part, err := writer.CreateFormFile("file", filepath.Base(path))
	if err != nil {
		return err
	}

	if _, err = io.Copy(part, file); err != nil {
			return err
	}

	if err = writer.Close(); err != nil {
			return err
	}

	req, err := http.NewRequest(http.MethodPost, url, &body)
	if err != nil {
			return err
	}

	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	result, err := io.ReadAll((io.LimitReader(resp.Body, 10<<20)))
	if err!= nil {
		return err
	}
	
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("Python вернул %d: %s", resp.StatusCode, result)
	}

	fmt.Println("Ответ Python:", string(result))
	return nil
}