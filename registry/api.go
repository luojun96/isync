package registry

import (
	"context"
	"io"

	manifestV2 "github.com/distribution/distribution/manifest/schema2"
	"github.com/opencontainers/go-digest"
)

type Registry interface {
	Ping() error
	ManifestV2(ctx context.Context, repo string, ref string) (manifestV2.DeserializedManifest, error)
	ManifestV2Exists(ctx context.Context, repo string, ref string) (bool, error)
	ManifestV2Put(ctx context.Context, repo string, ref string, manifest manifestV2.DeserializedManifest) error
	LayerExists(ctx context.Context, repo string, digest digest.Digest) (bool, error)
	LayerDownload(ctx context.Context, repo string, digest digest.Digest) (io.ReadCloser, error)
	LayerUpload(ctx context.Context, repo string, digest digest.Digest, reader io.Reader) error
	LayerMount(ctx context.Context, repo string, digest digest.Digest) error
}
