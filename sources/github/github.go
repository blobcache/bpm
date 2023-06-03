package github

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/blobcache/glfs"
	"github.com/blobcache/glfs/glfstar"
	"github.com/blobcache/glfs/glfszip"
	"github.com/brendoncarroll/go-exp/streams"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/stdctx/logctx"
	"github.com/google/go-github/v50/github"
	"golang.org/x/mod/semver"
	"golang.org/x/oauth2"

	"github.com/blobcache/bpm/bpmmd"
	"github.com/blobcache/bpm/sources"
)

var _ sources.Source = &GitHubSource{}

type GitHubSource struct {
	account string
	repo    string

	tokenSource oauth2.TokenSource
}

func NewGitHubSource(account, repo string) *GitHubSource {
	var tokenSource oauth2.TokenSource
	if v, ok := os.LookupEnv("GITHUB_TOKEN"); ok {
		tokenSource = oauth2.StaticTokenSource(
			&oauth2.Token{AccessToken: v},
		)
	}

	return &GitHubSource{
		account: account,
		repo:    repo,

		tokenSource: tokenSource,
	}
}

func (s *GitHubSource) newHTTPClient(ctx context.Context) *http.Client {
	if s.tokenSource == nil {
		return http.DefaultClient
	}
	return oauth2.NewClient(ctx, s.tokenSource)
}

func (s *GitHubSource) newClient(ctx context.Context) *github.Client {
	return github.NewClient(s.newHTTPClient(ctx))
}

const (
	tagPrefix   = "git-"
	assetPrefix = "ra-"
)

// Pull writes the asset to the store, and returns the root
func (s *GitHubSource) Pull(ctx context.Context, op *glfs.Operator, store cadata.Store, idstr string) (*glfs.Ref, error) {
	client := s.newClient(ctx)
	switch {
	case strings.HasPrefix(idstr, tagPrefix):
		id := strings.TrimPrefix(idstr, tagPrefix)
		u := fmt.Sprintf("https://api.github.com/repos/%s/%s/tarball/refs/tags/%s", s.account, s.repo, id)
		rc, err := download(ctx, u)
		if err != nil {
			return nil, err
		}
		return importGzipTAR(ctx, op, store, rc)

	case strings.HasPrefix(idstr, assetPrefix):
		id, err := strconv.ParseInt(strings.TrimPrefix(idstr, assetPrefix), 10, 64)
		if err != nil {
			return nil, err
		}
		ra, _, err := client.Repositories.GetReleaseAsset(ctx, s.account, s.repo, id)
		if err != nil {
			return nil, err
		}
		u := ra.GetBrowserDownloadURL()
		rc, err := download(ctx, u)
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		switch ra.GetContentType() {
		case "application/zip":
			return importZip(ctx, op, store, rc)
		case "application/x-gtar":
			return importGzipTAR(ctx, op, store, rc)
		case "application/gzip":
			r, err := gzip.NewReader(rc)
			if err != nil {
				return nil, err
			}
			defer r.Close()
			return importBlob(ctx, op, store, rc)
		default:
			return importBlob(ctx, op, store, rc)
		}

	default:
		return nil, fmt.Errorf("bad id %q", idstr)
	}
}

func importBlob(ctx context.Context, op *glfs.Operator, s cadata.Poster, r io.Reader) (*glfs.Ref, error) {
	w := op.NewBlobWriter(ctx, s)
	_, err := io.Copy(w, r)
	if err != nil {
		return nil, err
	}
	return w.Finish(ctx)
}

func importGzipTAR(ctx context.Context, op *glfs.Operator, s cadata.Poster, r io.Reader) (*glfs.Ref, error) {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	defer gr.Close()
	tr := tar.NewReader(gr)
	return glfstar.ReadTAR(ctx, op, s, tr)
}

func importZip(ctx context.Context, op *glfs.Operator, s cadata.Poster, r io.Reader) (*glfs.Ref, error) {
	f, err := os.CreateTemp(os.TempDir(), "bpm-import-zip")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	defer os.Remove(f.Name())
	size, err := io.Copy(f, r)
	if err != nil {
		return nil, err
	}
	zr, err := zip.NewReader(f, size)
	if err != nil {
		return nil, err
	}
	return glfszip.Import(ctx, op, s, zr)
}

func download(ctx context.Context, target string) (io.ReadCloser, error) {
	logctx.Infof(ctx, "downloading %v", target)
	res, err := http.Get(target)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		res.Body.Close()
		return nil, fmt.Errorf("non-zero status code %v", res.Status)
	}
	return res.Body, nil
}

func (s *GitHubSource) Fetch(ctx context.Context) (sources.AssetIterator, error) {
	it1 := &relAssetIterator{
		src: s,
	}
	it2 := &tagIterator{
		src: s,
	}
	return streams.Concat[sources.RemoteAsset](it1, it2), nil
}

type relAssetIterator struct {
	src *GitHubSource

	err         error
	relNextPage int
	results     []sources.RemoteAsset
}

func (it *relAssetIterator) Next(ctx context.Context, r *sources.RemoteAsset) error {
	if it.err != nil {
		return it.err
	}
	if len(it.results) == 0 {
		rels, err := it.listReleases(ctx, it.relNextPage)
		if err != nil {
			return err
		}
		if len(rels) == 0 {
			it.err = streams.EOS()
			return it.err
		}
		var results []sources.RemoteAsset
		for _, rel := range rels {
			for _, ass := range rel.Assets {
				labels := bpmmd.LabelSet{}
				if err := addReleaseLabels(labels, rel); err != nil {
					return err
				}
				if err := addAssetLabels(labels, ass); err != nil {
					return err
				}
				fuzzSemver(labels)
				fuzzArch(labels)
				fuzzOS(labels)
				results = append(results, sources.RemoteAsset{
					ID:     assetPrefix + strconv.FormatInt(ass.GetID(), 10),
					Labels: labels,
				})
			}
		}
		it.results = results
		it.relNextPage++
	}
	*r, it.results = it.results[0], it.results[1:]
	return nil
}

func (it *relAssetIterator) listReleases(ctx context.Context, page int) ([]*github.RepositoryRelease, error) {
	client := it.src.newClient(ctx)
	rels, _, err := client.Repositories.ListReleases(ctx, it.src.account, it.src.repo, &github.ListOptions{
		Page:    page,
		PerPage: 100,
	})
	return rels, err
}

type tagIterator struct {
	src *GitHubSource

	err      error
	nextPage int
	results  []sources.RemoteAsset
}

func (it *tagIterator) Next(ctx context.Context, dst *sources.RemoteAsset) error {
	if it.err != nil {
		return it.err
	}
	if len(it.results) == 0 {
		tags, err := it.listTags(ctx, it.nextPage)
		if err != nil {
			return err
		}
		if len(tags) == 0 {
			it.err = streams.EOS()
			return it.err
		}
		var results []sources.RemoteAsset
		for _, tag := range tags {
			labels := bpmmd.LabelSet{}
			if err := addTagLabels(labels, tag); err != nil {
				return err
			}
			fuzzSemver(labels)
			fuzzArch(labels)
			fuzzOS(labels)
			results = append(results, sources.RemoteAsset{
				ID:     tagPrefix + tag.GetName(),
				Labels: labels,
			})
		}
		it.results = results
		it.nextPage++
	}
	*dst, it.results = it.results[0], it.results[1:]
	return nil
}

func (s *tagIterator) listTags(ctx context.Context, page int) ([]*github.RepositoryTag, error) {
	client := s.src.newClient(ctx)
	tags, _, err := client.Repositories.ListTags(ctx, s.src.account, s.src.repo, &github.ListOptions{
		Page:    page,
		PerPage: 1000,
	})
	return tags, err
}

func addReleaseLabels(l bpmmd.LabelSet, rel *github.RepositoryRelease) error {
	l["tag_name"] = semver.Canonical(*rel.TagName)
	l["release_name"] = *rel.Name
	return nil
}

func addAssetLabels(l bpmmd.LabelSet, ass *github.ReleaseAsset) error {
	l["filename"] = ass.GetName()
	l["asset_id"] = strconv.Itoa(int(ass.GetID()))
	l["node_id"] = ass.GetNodeID()
	l["content_type"] = ass.GetContentType()
	addString(l, "label", ass.Label)
	return nil
}

func addTagLabels(l bpmmd.LabelSet, tag *github.RepositoryTag) error {
	l["git_tag"] = tag.GetName()
	commit := tag.GetCommit()
	addString(l, "git_sha", commit.SHA)
	addString(l, "git_message", commit.Message)
	return nil
}

func addString(l bpmmd.LabelSet, k string, x *string) {
	if x != nil {
		l[k] = *x
	}
}

func fuzzSemver(l bpmmd.LabelSet) {
	t, ok := l["tag_name"]
	if !ok {
		return
	}
	sv := semver.Canonical(t)
	if sv != "" {
		l["semver"] = sv
	}
}

func fuzzArch(l bpmmd.LabelSet) {
	name, ok := l["filename"]
	if !ok {
		return
	}
	for _, x := range []string{
		"amd64",
		"arm64",
		"aarch64",
		"riscv",
	} {
		if strings.Contains(name, x) {
			l["arch"] = x
		}
	}
}

func fuzzOS(l bpmmd.LabelSet) {
	name, ok := l["filename"]
	if !ok {
		return
	}
	for _, x := range []string{
		"linux",
		"darwin",
		"windows",
	} {
		if strings.Contains(name, x) {
			l["os"] = x
		}
	}
}
