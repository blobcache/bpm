package httpscrape

import (
	"context"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/blobcache/glfs"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/streams"
	"github.com/gocolly/colly/v2"

	"github.com/blobcache/bpm/bpmmd"
	"github.com/blobcache/bpm/sources"
)

type HTTPScraper struct {
	target url.URL
}

func NewHTTPScraper(target string) (*HTTPScraper, error) {
	u, err := url.Parse(target)
	if err != nil {
		return nil, err
	}
	u.Scheme = "https"
	return &HTTPScraper{target: *u}, nil
}

func (s *HTTPScraper) Fetch(ctx context.Context) (sources.AssetIterator, error) {
	hc := http.DefaultClient
	resp, err := hc.Get(s.target.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	c := colly.NewCollector()
	var assets []sources.RemoteAsset
	c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		link := e.Attr("href")
		link = e.Request.AbsoluteURL(link)
		if !strings.HasPrefix(link, s.target.String()) {
			return
		}
		id := strings.TrimPrefix(link, s.target.String())

		assets = append(assets, sources.RemoteAsset{
			ID: id,
			Labels: bpmmd.LabelSet{
				"name":     e.Text,
				"filename": path.Base(link),
			},
		})
	})
	if err := c.Visit(s.target.String()); err != nil {
		return nil, err
	}
	return streams.NewSlice(assets, nil), nil
}

func (s *HTTPScraper) Pull(ctx context.Context, fsop *glfs.Operator, src cadata.Store, id string) (*glfs.Ref, error) {
	u2 := s.target
	u2.Path = path.Join(u2.Path, id)

	switch {
	case strings.HasSuffix(u2.Path, ".tar.gz"):
		panic("not implemented")
	case strings.HasSuffix(u2.Path, ".zip"):
		panic("not implemented")
	default:
		return nil, nil
	}
}
