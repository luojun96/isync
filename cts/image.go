package cts

import (
	"github.com/distribution/distribution"
	manifestV2 "github.com/distribution/distribution/manifest/schema2"
)

type Image struct {
	Name     string
	Tag      string
	Exists   bool
	Manifest manifestV2.DeserializedManifest
	Layers   []*ImageLayer
}

type ImageLayer struct {
	Ref    Image
	Layer  distribution.Descriptor
	Exists bool
	Synced bool
}
