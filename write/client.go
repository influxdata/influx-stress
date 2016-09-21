package write

import (
	"fmt"
	"net/url"
	"time"

	"github.com/valyala/fasthttp"
)

const (
	DefaultDatabase        = "stress"
	DefaultRetentionPolicy = "autogen"
)

type Client interface {
	Send([]byte) (int64, int, error)
}

type client struct {
	url []byte
}

// add consistency and config struct
func NewClient(influxURL, db, rp, p string) *client {
	tmpltURL := "%s/write?db=%s&rp=%s&precision=%s"
	u := fmt.Sprintf(tmpltURL,
		influxURL, url.QueryEscape(db), url.QueryEscape(rp), url.QueryEscape(p))
	return &client{url: []byte(u)}
}

func (c *client) Send(b []byte) (latNs int64, statusCode int, err error) {
	req := fasthttp.AcquireRequest()
	req.Header.SetContentTypeBytes([]byte("text/plain"))
	req.Header.SetMethodBytes([]byte("POST"))
	req.Header.SetRequestURIBytes(c.url)
	req.SetBody(b)

	resp := fasthttp.AcquireResponse()
	start := time.Now()

	err = fasthttp.Do(req, resp)
	latNs = time.Since(start).Nanoseconds()
	statusCode = resp.StatusCode()

	fasthttp.ReleaseResponse(resp)
	fasthttp.ReleaseRequest(req)

	return
}
