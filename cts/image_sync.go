package cts

import (
	"context"
	"fmt"
	"log"
	"strings"

	manifestV2 "github.com/distribution/distribution/manifest/schema2"
	"github.com/luojun96/isync/pool"
	"github.com/luojun96/isync/registry"
)

const Concurrency int = 3

type imageTask struct {
	image Image
	s     *imageSync
	exec  func(ctx context.Context, image Image, s *imageSync) error
	err   error
}

func (t *imageTask) Execute(ctx context.Context) {
	t.err = t.exec(ctx, t.image, t.s)
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
	log.Println("start to push images...")
	images, err := s.initImages(artifacts)
	if err != nil {
		return fmt.Errorf("failed to initialize images: %v", err)
	}

	fullImages, err := s.getFullImages(ctx, images)
	if err != nil {
		return fmt.Errorf("failed to get full images: %v", err)
	}

	filteredImages, err := s.filterImages(ctx, fullImages)

	layers := s.getLayers(filteredImages)

	s.pushLayers(ctx, layers)
	s.mountLayers(ctx, layers)
	s.createManifests(ctx, filteredImages)

	return nil
}

func (s *imageSync) initImages(artifacts []string) ([]Image, error) {
	log.Println("initialize images...")
	var images []Image
	for _, image := range artifacts {
		tokens := strings.Split(image, ":")
		if len(tokens) != 2 {
			return nil, fmt.Errorf("Invalidate image type %s", image)
		}
		images = append(images, Image{tokens[0], tokens[1], false, manifestV2.DeserializedManifest{}, nil})
	}
	return images, nil
}

func (s *imageSync) getFullImages(ctx context.Context, images []Image) ([]Image, error) {
	log.Println("get full images...")
	fullImages := []Image{}
	p := pool.NewWorkPool(Concurrency)
	imageTasks := make([]imageTask, 0)
	for _, image := range images {
		task := &imageTask{
			image: image,
			s:     s,
			exec: func(ctx context.Context, image Image, s *imageSync) error {
				log.Printf("start to check manifest exists of %s:%s", image.Name, image.Tag)
				exists, err := s.dr.ManifestsExists(ctx, image.Name, image.Tag)
				image.Exists = exists
				if err != nil {
					return fmt.Errorf("failed to check manifest exists of %s:%s: %v", image.Name, image.Tag, err)
				}
				if !exists {
					log.Printf("manifest of %s:%s not exists", image.Name, image.Tag)
				}
				return nil
			},
		}
		imageTasks = append(imageTasks, *task)
		p.AddTask(task)
	}
	p.Run(ctx)

	for _, task := range imageTasks {
		if task.err != nil {
			return nil, task.err
		}
		fullImages = append(fullImages, task.image)
	}

	return fullImages, nil
}

func (s *imageSync) filterImages(ctx context.Context, images []Image) ([]Image, error) {
	log.Println("filter images...")
	filteredImages := []Image{}
	return filteredImages, nil
}

func (s *imageSync) getLayers(images []Image) []ImageLayer {
	log.Println("get image layers...")
	return []ImageLayer{}
}

func (s *imageSync) pushLayers(ctx context.Context, layers []ImageLayer) error {
	log.Println("push image layers...")
	return nil
}

func (s *imageSync) mountLayers(ctx context.Context, layers []ImageLayer) error {
	log.Println("mount image layers...")
	return nil
}

func (s *imageSync) createManifests(ctx context.Context, images []Image) error {
	log.Println("create manifests...")
	return nil
}
