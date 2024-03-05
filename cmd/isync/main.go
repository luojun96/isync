package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/luojun96/isync/cts"
	"github.com/luojun96/isync/registry"
)

func main() {
	ctx := context.Background()

	artifacts := os.Getenv("ARTIFACTS")
	if artifacts == "" {
		log.Fatal("No artifacts to be pushed.")
	}

	sr := registry.NewRegistry("https://registry.jun.com/")
	if err := sr.Ping(); err != nil {
		log.Fatal(fmt.Errorf("failed to ping source registry: %v", err))
	}

	dr := registry.NewRegistry("http://aliyun:5000/")
	if err := dr.Ping(); err != nil {
		log.Fatal(fmt.Errorf("failed to ping destination registry: %v", err))
	}

	if err := cts.NewImageSync(sr, dr).Sync(ctx, strings.Split(artifacts, ",")); err != nil {
		log.Fatal(fmt.Errorf("failed to sync images: %v", err))
	}
}
