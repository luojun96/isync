package cts

import (
	manifestV2 "github.com/distribution/distribution/manifest/schema2"
	"github.com/docker/distribution"
)

type Image struct {
	Name     string
	Tag      string
	Exists   bool
	Manifest manifestV2.DeserializedManifest
	Layers   []Layer
}

type Layer struct {
	Ref        Image
	Descriptor distribution.Descriptor
	Exists     bool
	Synced     bool
}
