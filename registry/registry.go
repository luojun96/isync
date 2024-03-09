package registry

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
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

func (r *DockerRegistry) urlf(format string, a ...interface{}) string {
	return fmt.Sprintf(r.url(format), a...)
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

func (r *DockerRegistry) ManifestV2(ctx context.Context, repo string, ref string) (manifestV2.DeserializedManifest, error) {
	url := r.url(fmt.Sprintf("/v2/%s/manifests/%s", repo, ref))
	log.Printf("registry: fetching manifest from %s", url)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return manifestV2.DeserializedManifest{}, err
	}

	req.Header.Set("Accept", manifestV2.MediaTypeManifest)
	resp, err := ctxhttp.Do(ctx, r.Client, req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return manifestV2.DeserializedManifest{}, err
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return manifestV2.DeserializedManifest{}, err
	}

	if resp.StatusCode != http.StatusOK {
		return manifestV2.DeserializedManifest{}, fmt.Errorf("failed to fetch manifest %s:%s, status code: %d,error message: %s", repo, ref, resp.StatusCode, string(data))
	}

	d := manifestV2.DeserializedManifest{}
	if err := d.UnmarshalJSON(data); err != nil {
		return manifestV2.DeserializedManifest{}, err
	}
	return d, nil
}

func (r *DockerRegistry) ManifestV2Put(ctx context.Context, repo string, ref string, manifest manifestV2.DeserializedManifest) error {
	url := r.urlf("/v2/%s/manifests/%s", repo, ref)
	log.Printf("registry: putting manifest to %s", url)
	data, err := manifest.MarshalJSON()
	if err != nil {
		return err
	}
	buffer := bytes.NewBuffer(data)
	req, err := http.NewRequest(http.MethodPut, url, buffer)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", manifestV2.MediaTypeManifest)
	resp, err := ctxhttp.Do(ctx, r.Client, req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("failed to put manifest of image %s:%s, status code: %d", repo, ref, resp.StatusCode)
	}
	return nil
}

func (r *DockerRegistry) LayerExists(ctx context.Context, repo string, digest digest.Digest) (bool, error) {
	url := r.urlf("/v2/%s/blobs/%s", repo, digest)
	log.Printf("registry: checking layer exists %s", url)

	resp, err := ctxhttp.Head(ctx, r.Client, url)
	if resp != nil {
		defer resp.Body.Close()
	}

	if err == nil && resp.StatusCode == http.StatusOK {
		return true, nil
	}

	if err != nil {
		return false, err
	}

	return false, nil
}

func (r *DockerRegistry) LayerDownload(ctx context.Context, repo string, digest digest.Digest) (io.ReadCloser, error) {
	url := r.urlf("/v2/%s/blobs/%s", repo, digest)
	log.Printf("registry: downloading layer from %s", url)
	resp, err := ctxhttp.Get(ctx, r.Client, url)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (r *DockerRegistry) LayerUpload(ctx context.Context, repo string, digest digest.Digest, reader io.Reader) error {
	url, err := r.initiateUpload(ctx, repo)
	if err != nil {
		return err
	}
	query := url.Query()
	query.Set("digest", digest.String())
	url.RawQuery = query.Encode()
	log.Printf("registry: uploading layer to %s", url.String())

	locationURL := url.String()[strings.Index(url.String(), "v2")-1:]
	uploadURL := r.url(locationURL)
	req, err := http.NewRequest(http.MethodPut, uploadURL, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := ctxhttp.Do(ctx, r.Client, req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("failed to upload layer %s, status code: %d, location url: %v", digest, resp.StatusCode, locationURL)
	}

	return nil
}

func (r *DockerRegistry) LayerMount(ctx context.Context, repo string, digest digest.Digest) error {
	mountURL := r.urlf("/v2/%s/blobs/uploads/", repo)
	log.Printf("registry: mounting layer to %s", mountURL)
	values := url.Values{}
	values.Add("mount", digest.String())
	values.Add("from", "trunk")
	resp, err := ctxhttp.PostForm(ctx, r.Client, mountURL, values)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("failed to mount layer %s of repository %s, status code: %d", digest, repo, resp.StatusCode)
	}
	return nil
}

func (r *DockerRegistry) initiateUpload(ctx context.Context, repo string) (*url.URL, error) {
	initiateURL := r.urlf("/v2/%s/blobs/uploads/", repo)
	log.Printf("registry: initiating upload to %s", initiateURL)
	resp, err := ctxhttp.Post(ctx, r.Client, initiateURL, "application/octet-stream", nil)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusAccepted {
		return nil, fmt.Errorf("failed to initiate upload to %s, status code: %d", initiateURL, resp.StatusCode)
	}

	location := resp.Header.Get("Location")
	if location == "" {
		return nil, fmt.Errorf("failed to initiate upload to %s, location header is empty", initiateURL)
	}
	locationURL, err := url.Parse(location)
	if err != nil {
		return nil, err
	}
	return locationURL, nil
}
