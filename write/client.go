package write

import (
	"fmt"
	"net/url"
	"time"

	"github.com/valyala/fasthttp"
)

type client struct {
	url []byte
}

func NewClient(influxURL, db, p string) *client {
	tmpltURL := "%s/write?db=%s&precision=%s"
	u := fmt.Sprintf(tmpltURL, influxURL, url.QueryEscape(db), url.QueryEscape(p))
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
