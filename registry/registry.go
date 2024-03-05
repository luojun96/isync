package registry

import (
	"context"
	"fmt"
	"net/http"
	"strings"

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

func (r *DockerRegistry) ManifestsExists(ctx context.Context, repo string, ref string) (bool, error) {
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
