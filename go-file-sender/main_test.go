package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testClient() *http.Client {
    return &http.Client{Timeout: 5 * time.Second}
}

func testFile(t *testing.T, name string, data []byte) string {
    t.Helper()

    path := filepath.Join(t.TempDir(), name)
    if err := os.WriteFile(path, data, 0600); err != nil {
        t.Fatal(err)
    }
    return path
}

// GO-01: Проверка multipart/form-data
func TestUploadMultipart(t *testing.T) {
    path := testFile(t, "document.pdf", []byte("%PDF-test"))
    called := false

    server := httptest.NewServer(http.HandlerFunc(
        func(w http.ResponseWriter, r *http.Request) {
            called = true

            if r.Method != http.MethodPost {
                t.Errorf("Expected POST, got %s", r.Method)
            }

            if !strings.HasPrefix(
                r.Header.Get("Content-Type"),
                "multipart/form-data;",
            ) {
                t.Error("Invalid Content-Type")
            }

            file, header, err := r.FormFile("file")
            if err != nil {
                t.Errorf("Missing form field 'file': %v", err)
                w.WriteHeader(http.StatusBadRequest)
                return
            }
            defer file.Close()

            if header.Filename != "document.pdf" {
                t.Errorf("Unexpected filename: %s", header.Filename)
            }

            w.WriteHeader(http.StatusAccepted)
            fmt.Fprint(w, `{"status":"queued"}`)
        },
    ))
    defer server.Close()

    if err := sendFile(testClient(), server.URL, path); err != nil {
        t.Fatal(err)
    }
    if !called {
        t.Fatal("Server did not receive the request")
    }
}

// GO-02: Бинарные данные передаются без изменений
func TestUploadBinaryIntegrity(t *testing.T) {
    original := []byte{0x00, 0xFF, 0x89, 0x50, 0x4E, 0x47, 0x00}
    path := testFile(t, "image.png", original)

    server := httptest.NewServer(http.HandlerFunc(
        func(w http.ResponseWriter, r *http.Request) {
            file, _, err := r.FormFile("file")
            if err != nil {
                t.Error(err)
                w.WriteHeader(http.StatusBadRequest)
                return
            }
            defer file.Close()

            received, err := io.ReadAll(file)
            if err != nil {
                t.Error(err)
                w.WriteHeader(http.StatusBadRequest)
                return
            }

            if !bytes.Equal(received, original) {
                t.Error("File contents changed during upload")
            }
            w.WriteHeader(http.StatusAccepted)
        },
    ))
    defer server.Close()

    if err := sendFile(testClient(), server.URL, path); err != nil {
        t.Fatal(err)
    }
}

// GO-03: HTTP 202 считается успешной отправкой
func TestUploadAccepted202(t *testing.T) {
    testUploadStatus(t, http.StatusAccepted, false)
}

// GO-04: HTTP 200 тоже считается успешным
func TestUploadSuccess200(t *testing.T) {
    testUploadStatus(t, http.StatusOK, false)
}

// GO-05: Python отклонил запрос (400)
func TestUploadBadRequest400(t *testing.T) {
    testUploadStatus(t, http.StatusBadRequest, true)
}

// GO-06: Файл слишком большой (413)
func TestUploadTooLarge413(t *testing.T) {
    testUploadStatus(t, http.StatusRequestEntityTooLarge, true)
}

// GO-07: Kafka недоступна, Python вернул 503
func TestUploadUnavailable503(t *testing.T) {
    testUploadStatus(t, http.StatusServiceUnavailable, true)
}

func testUploadStatus(t *testing.T, status int, expectError bool) {
    t.Helper()

    path := testFile(t, "test.pdf", []byte("test"))

    server := httptest.NewServer(http.HandlerFunc(
        func(w http.ResponseWriter, r *http.Request) {
            w.WriteHeader(status)
            fmt.Fprint(w, `{"status":"test"}`)
        },
    ))
    defer server.Close()

    err := sendFile(testClient(), server.URL, path)

    if expectError && err == nil {
        t.Fatalf("Expected error for HTTP %d", status)
    }

    if !expectError && err != nil {
        t.Fatalf("Unexpected error for HTTP %d: %v", status, err)
    }

    if expectError && !strings.Contains(
        err.Error(), fmt.Sprint(status),
    ) {
        t.Errorf("Error does not contain HTTP status %d: %v", status, err)
    }
}

// GO-08: Локальный файл отсутствует
func TestUploadMissingFile(t *testing.T) {
    path := filepath.Join(t.TempDir(), "missing.pdf")

    err := sendFile(
        testClient(),
        "http://127.0.0.1:8000/upload",
        path,
    )

    if err == nil {
        t.Fatal("Expected error for missing file")
    }
}

// GO-09: Сервер Python недоступен
func TestUploadConnectionFailure(t *testing.T) {
    path := testFile(t, "document.pdf", []byte("test"))

    server := httptest.NewServer(http.HandlerFunc(
        func(w http.ResponseWriter, r *http.Request) {},
    ))
    url := server.URL
    server.Close()

    err := sendFile(testClient(), url, path)
    if err == nil {
        t.Fatal("Expected connection error")
    }
}
