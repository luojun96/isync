package cts

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"strings"
	"time"

	manifestV2 "github.com/distribution/distribution/manifest/schema2"
	"github.com/luojun96/isync/pool"
	"github.com/luojun96/isync/registry"
)

const Concurrency int = 3

var concurrency int

type task[T any] struct {
	t    T
	s    ArtifactSync
	exec func(ctx context.Context, t T, s ArtifactSync) error
	err  error
}

func (t *task[T]) Execute(ctx context.Context) {
	t.err = t.exec(ctx, t.t, t.s)
}

type imageSync struct {
	sr registry.Registry
	dr registry.Registry
}

func NewImageSync(sr registry.Registry, dr registry.Registry) ArtifactSync {
	return &imageSync{
		sr: sr,
		dr: dr,
	}
}

func (s *imageSync) Sync(ctx context.Context, artifacts []string) error {
	start := time.Now()

	concurrency = runtime.GOMAXPROCS(0)
	log.Printf("concurrency: %d\n", concurrency)
	images, err := s.getImages(ctx, artifacts)
	if err != nil {
		return fmt.Errorf("failed to get images: %v", err)
	}

	if err = s.initImages(ctx, images); err != nil {
		return fmt.Errorf("failed to initialize images: %v", err)
	}

	var imagesToPush []*Image
	for _, image := range images {
		if !image.Exists {
			imagesToPush = append(imagesToPush, image)
		}
	}

	if len(imagesToPush) == 0 {
		log.Printf("[%vs] All images already exist in destination registry, skipped to push.\n", int(time.Since(start).Seconds()))
		return nil
	}
	log.Printf("[%vs] %d of %d images do not exist in destination registry, will be pushed.\n", int(time.Since(start).Seconds()), len(imagesToPush), len(images))

	if err = s.setManifest(ctx, imagesToPush); err != nil {
		return fmt.Errorf("failed to set manifest: %v", err)
	}

	layers := s.getLayers(imagesToPush)
	if err = s.initLayers(ctx, layers); err != nil {
		return fmt.Errorf("failed to initialize layers: %v", err)
	}

	var layersToPush []*Layer
	for _, layer := range layers {
		if !layer.Exists {
			layersToPush = append(layersToPush, layer)
		}
	}

	log.Printf("[%vs] %d of %d layers do not exist in destination registry, will be pushed.\n", int(time.Since(start).Seconds()), len(layersToPush), len(layers))

	if err = s.pushLayers(ctx, layersToPush); err != nil {
		return fmt.Errorf("failed to push layers: %v", err)
	}

	if err = s.mountLayers(ctx, layersToPush); err != nil {
		return fmt.Errorf("failed to mount layers: %v", err)
	}

	if err = s.createManifests(ctx, imagesToPush); err != nil {
		return fmt.Errorf("failed to create manifests: %v", err)
	}

	// check if all images are pushed successfully
	if err = s.checkImages(ctx, imagesToPush); err != nil {
		return fmt.Errorf("failed to check images: %v", err)
	}

	log.Printf("[%vs] All images are pushed to destination registry successfully.\n", int(time.Since(start).Seconds()))
	return nil
}

func (s *imageSync) getImages(ctx context.Context, artifacts []string) ([]*Image, error) {
	images := []*Image{}
	for _, image := range artifacts {
		tokens := strings.Split(image, ":")
		if len(tokens) != 2 {
			return nil, fmt.Errorf("Invalidate image type %s", image)
		}
		images = append(images, &Image{tokens[0], tokens[1], false, manifestV2.DeserializedManifest{}, nil})
	}
	return images, nil
}

func (s *imageSync) initImages(ctx context.Context, images []*Image) error {
	log.Println("initialize images...")
	var handler = func(ctx context.Context, image *Image, s ArtifactSync) error {
		var err error
		image.Exists, err = s.Destination().ManifestV2Exists(ctx, image.Name, image.Tag)
		if err != nil {
			return fmt.Errorf("failed to check manifest exists of %s:%s: %v", image.Name, image.Tag, err)
		}

		if image.Tag == "latest" {
			srcManifest, err := s.Source().ManifestV2(ctx, image.Name, "latest")
			if err != nil {
				return fmt.Errorf("failed to get manifest of %s:%s in source registry: %v", image.Name, image.Tag, err)
			}
			destManifest, err := s.Destination().ManifestV2(ctx, image.Name, "latest")
			if err == nil && destManifest.Config.Digest == srcManifest.Config.Digest {
				image.Exists = true
			}
		}
		if image.Exists {
			log.Printf("image %s:%s already exists in destination registry, skipped to push.\n", image.Name, image.Tag)
		} else {
			log.Printf("Image %s:%s does not exist in destination registry, will be pushed.\n", image.Name, image.Tag)
		}
		return nil
	}

	p := pool.NewWorkPool(Concurrency)
	imageTasks := make([]pool.Task, 0)
	for _, image := range images {
		t := &task[*Image]{
			t:    image,
			s:    s,
			exec: handler,
		}
		imageTasks = append(imageTasks, t)
		p.AddTask(t)
	}
	p.Run(ctx)

	for _, t := range imageTasks {
		item, ok := t.(*task[*Image])
		if !ok {
			return errors.New("failed to convert task to *task[*Image]")
		}
		if item.err != nil {
			return item.err
		}
	}
	return nil
}

func (s *imageSync) setManifest(ctx context.Context, images []*Image) error {
	log.Println("set the manifest of images...")
	var handler = func(ctx context.Context, image *Image, s ArtifactSync) error {
		var err error
		image.Manifest, err = s.Source().ManifestV2(ctx, image.Name, image.Tag)
		if err != nil {
			return fmt.Errorf("failed to get manifest of %s:%s: %v", image.Name, image.Tag, err)
		}
		return nil
	}

	p := pool.NewWorkPool(Concurrency)
	imageTasks := make([]pool.Task, 0)
	for _, image := range images {
		t := &task[*Image]{
			t:    image,
			s:    s,
			exec: handler,
		}
		imageTasks = append(imageTasks, t)
		p.AddTask(t)
	}
	p.Run(ctx)

	for _, t := range imageTasks {
		item, ok := t.(*task[*Image])
		if !ok {
			return errors.New("failed to convert task to *task[*Image]")
		}
		if item.err != nil {
			return item.err
		}
	}

	return nil
}

func (s *imageSync) getLayers(images []*Image) []*Layer {
	log.Println("get image layers...")
	var layers []*Layer
	for _, image := range images {
		layers = append(layers, &Layer{*image, image.Manifest.Config, false, false})
		for _, layer := range image.Manifest.Manifest.Layers {
			layers = append(layers, &Layer{*image, layer, false, false})
		}
	}
	return layers
}

func (s *imageSync) initLayers(ctx context.Context, layers []*Layer) error {
	log.Println("initialize layers...")
	var handler = func(ctx context.Context, layer *Layer, s ArtifactSync) error {
		var err error
		layer.Exists, err = s.Destination().LayerExists(ctx, "trunk", layer.Descriptor.Digest)
		if err != nil {
			return fmt.Errorf("failed to check layer exists of %s:%s: %v", layer.Ref.Name, layer.Descriptor.Digest, err)
		}
		if layer.Exists {
			log.Printf("layer %s:%s already exists in destination registry, skipped to push.\n", layer.Ref.Name, layer.Descriptor.Digest)
		} else {
			log.Printf("layer %s:%s does not exist in destination registry, will be pushed.\n", layer.Ref.Name, layer.Descriptor.Digest)
		}
		return nil
	}

	p := pool.NewWorkPool(Concurrency)
	layerTasks := make([]pool.Task, 0)
	for _, layer := range layers {
		t := &task[*Layer]{
			t:    layer,
			s:    s,
			exec: handler,
		}
		layerTasks = append(layerTasks, t)
		p.AddTask(t)
	}
	p.Run(ctx)

	return nil
}

func (s *imageSync) pushLayers(ctx context.Context, layers []*Layer) error {
	log.Println("push image layers...")
	var handler = func(ctx context.Context, layer *Layer, s ArtifactSync) error {
		if layer.Exists {
			return nil
		}
		log.Printf("push layer %s:%s to destination registry...\n", layer.Ref.Name, layer.Descriptor.Digest)
		start := time.Now()
		// push single layer
		digest, err := s.Source().LayerDownload(ctx, layer.Ref.Name, layer.Descriptor.Digest)
		if err != nil {
			return fmt.Errorf("failed to download layer %s:%s: %v", layer.Ref.Name, layer.Descriptor.Digest, err)
		}
		if digest != nil {
			defer digest.Close()
		}
		if err = s.Destination().LayerUpload(ctx, "trunk", layer.Descriptor.Digest, digest); err != nil {
			return fmt.Errorf("failed to upload layer %s:%s: %v", layer.Ref.Name, layer.Descriptor.Digest, err)
		}
		layer.Synced = true
		elapse := int(time.Since(start).Seconds())
		speed := float64(layer.Descriptor.Size) / 1024 / 1024 / float64(elapse)
		log.Printf("push layer %s:%s to destination registry successfully, elapse %ds, speed %.2fMB/s.\n", layer.Ref.Name, layer.Descriptor.Digest, elapse, speed)
		return nil
	}

	p := pool.NewWorkPool(Concurrency)
	layerTasks := make([]pool.Task, 0)
	for _, layer := range layers {
		t := &task[*Layer]{
			t:    layer,
			s:    s,
			exec: handler,
		}
		layerTasks = append(layerTasks, t)
		p.AddTask(t)
	}
	p.Run(ctx)

	for _, t := range layerTasks {
		item, ok := t.(*task[*Layer])
		if !ok {
			return errors.New("failed to convert task to *task[*Layer]")
		}
		if item.err != nil {
			return item.err
		}

	}
	return nil
}

func (s *imageSync) mountLayers(ctx context.Context, layers []*Layer) error {
	log.Println("mount image layers...")
	var handler = func(ctx context.Context, layer *Layer, s ArtifactSync) error {
		if !layer.Synced {
			return fmt.Errorf("layer %s:%s has not been pushed to destination registry", layer.Ref.Name, layer.Descriptor.Digest)
		}
		log.Printf("mount layer %s:%s to destination registry...\n", layer.Ref.Name, layer.Descriptor.Digest)
		if err := s.Destination().LayerMount(ctx, layer.Ref.Name, layer.Descriptor.Digest); err != nil {
			log.Printf("failed to mount layer %s:%s to destination registry: %v\n", layer.Ref.Name, layer.Descriptor.Digest, err)
		} else {
			log.Printf("mount layer %s:%s to destination registry successfully.\n", layer.Ref.Name, layer.Descriptor.Digest)
		}
		return nil
	}

	p := pool.NewWorkPool(Concurrency)
	layerTasks := make([]pool.Task, 0)
	for _, layer := range layers {
		t := &task[*Layer]{
			t:    layer,
			s:    s,
			exec: handler,
		}
		layerTasks = append(layerTasks, t)
		p.AddTask(t)
	}
	p.Run(ctx)

	for _, t := range layerTasks {
		item, ok := t.(*task[*Layer])
		if !ok {
			return errors.New("failed to convert task to *task[*Layer]")
		}
		if item.err != nil {
			return item.err
		}
	}

	return nil
}

func (s *imageSync) createManifests(ctx context.Context, images []*Image) error {
	log.Println("create manifests...")
	var handler = func(ctx context.Context, image *Image, s ArtifactSync) error {
		log.Printf("create manifest of image %s:%s in destination registry...\n", image.Name, image.Tag)
		if err := s.Destination().ManifestV2Put(ctx, image.Name, image.Tag, image.Manifest); err != nil {
			return fmt.Errorf("failed to put manifest %s:%s: %v", image.Name, image.Tag, err)
		}
		log.Printf("create manifest of image %s:%s in destination registry successfully.\n", image.Name, image.Tag)
		return nil
	}

	p := pool.NewWorkPool(Concurrency)
	imageTasks := make([]pool.Task, 0)
	for _, image := range images {
		t := &task[*Image]{
			t:    image,
			s:    s,
			exec: handler,
		}
		imageTasks = append(imageTasks, t)
		p.AddTask(t)
	}
	p.Run(ctx)

	for _, t := range imageTasks {
		item, ok := t.(*task[*Image])
		if !ok {
			return errors.New("failed to convert task to *task[*Image]")
		}
		if item.err != nil {
			return item.err
		}
	}
	return nil
}

func (s *imageSync) checkImages(ctx context.Context, images []*Image) error {
	log.Println("check images...")
	var handler = func(ctx context.Context, image *Image, s ArtifactSync) error {
		exists, err := s.Destination().ManifestV2Exists(ctx, image.Name, image.Tag)
		if err != nil {
			return fmt.Errorf("failed to check manifest exists of %s:%s: %v", image.Name, image.Tag, err)
		}

		if image.Tag == "latest" {
			destManifest, err := s.Destination().ManifestV2(ctx, image.Name, "latest")
			if err == nil && destManifest.Config.Digest == image.Manifest.Config.Digest {
				exists = true
			}
		}

		if !exists {
			log.Printf("the manifest of image %s:%s does not exist in destination registry, failed to push.\n", image.Name, image.Tag)
			return fmt.Errorf("the manifest of image %s:%s does not exist in destination registry", image.Name, image.Tag)
		}
		return nil
	}

	p := pool.NewWorkPool(Concurrency)
	imageTasks := make([]pool.Task, 0)
	for _, image := range images {
		t := &task[*Image]{
			t:    image,
			s:    s,
			exec: handler,
		}
		imageTasks = append(imageTasks, t)
		p.AddTask(t)
	}

	p.Run(ctx)

	for _, t := range imageTasks {
		item, ok := t.(*task[*Image])
		if !ok {
			return errors.New("failed to convert task to *task[*Image]")
		}
		if item.err != nil {
			return fmt.Errorf("failed to check image %s:%s: %v", item.t.Name, item.t.Tag, item.err)
		}
	}

	return nil
}

func (s *imageSync) Source() registry.Registry {
	return s.sr
}

func (s *imageSync) Destination() registry.Registry {
	return s.dr
}
