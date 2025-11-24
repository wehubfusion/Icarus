package tests

import (
	"testing"

	"github.com/wehubfusion/Icarus/pkg/storage"
	"go.uber.org/zap"
)

func TestNewAzureBlobClientValidatesLogger(t *testing.T) {
	_, err := storage.NewAzureBlobClient("UseDevelopmentStorage=true;", "container", nil)
	if err == nil {
		t.Fatal("expected error when logger is nil")
	}
}

func TestNewAzureBlobClientValidatesConnectionString(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	_, err := storage.NewAzureBlobClient("", "container", logger)
	if err == nil {
		t.Fatal("expected error when connection string is empty")
	}
}

func TestNewAzureBlobClientValidatesContainerName(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	_, err := storage.NewAzureBlobClient("UseDevelopmentStorage=true;", "", logger)
	if err == nil {
		t.Fatal("expected error when container name is empty")
	}
}
