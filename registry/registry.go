package registry

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	manifestV2 "github.com/distribution/distribution/manifest/schema2"
	"github.com/opencontainers/go-digest"
	"golang.org/x/net/context/ctxhttp"
)

type DockerRegistry struct {
	URL    string
	Client *http.Client
}

func NewRegistry(url string) Registry {
	u := strings.TrimSuffix(url, "/")
	return &DockerRegistry{
		URL: u,
		Client: &http.Client{
			Transport: http.DefaultTransport,
		},
	}
}

func (r *DockerRegistry) Ping() error {
	url := r.url("/v2/")
	resp, err := r.Client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ping failed with status code: %d", resp.StatusCode)
	}
	return nil
}

func (r *DockerRegistry) url(suffix string) string {
	return fmt.Sprintf("%s%s", r.URL, suffix)
}

func (r *DockerRegistry) ManifestV2Exists(ctx context.Context, repo string, ref string) (bool, error) {
	url := r.url(fmt.Sprintf("/v2/%s/manifests/%s", repo, ref))
	resp, err := ctxhttp.Head(ctx, r.Client, url)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err == nil && resp.StatusCode == http.StatusOK {
		// to-do
		// compare the digest of image
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return false, nil
}

func (r *DockerRegistry) ManifestV2(ctx context.Context, repo string, ref string) (*manifestV2.DeserializedManifest, error) {
	return nil, nil
}

func (r *DockerRegistry) manifestV2Put(ctx context.Context, repo string, ref string, manifest *manifestV2.DeserializedManifest) error {
	return nil
}

func (r *DockerRegistry) LayerExists(ctx context.Context, repo string, digest digest.Digest) (bool, error) {
	return false, nil
}

func (r *DockerRegistry) LayerDownload(ctx context.Context, repo string, digest digest.Digest) (io.ReadCloser, error) {
	return nil, nil
}

func (r *DockerRegistry) LayerUpload(ctx context.Context, repo string, digest digest.Digest, reader io.Reader) error {
	return nil
}

func (r *DockerRegistry) LayerMount(ctx context.Context, repo string, digest digest.Digest) error {
	return nil
}
