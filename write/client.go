package write

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/valyala/fasthttp"
)

const (
	DefaultDatabase        = "stress"
	DefaultRetentionPolicy = "autogen"
)

type ClientConfig struct {
	BaseURL string

	Database        string
	RetentionPolicy string
	Precision       string
	Consistency     string
}

type Client interface {
	Create(string) error
	Send([]byte) (int64, int, error)
}

type client struct {
	url []byte

	cfg ClientConfig
}

func NewClient(cfg ClientConfig) Client {
	return &client{
		url: []byte(writeURLFromConfig(cfg)),
		cfg: cfg,
	}
}

func (c *client) Create(command string) error {
	if command == "" {
		command = "CREATE DATABASE " + c.cfg.Database
	}

	vals := url.Values{}
	vals.Set("q", command)
	resp, err := http.PostForm(c.cfg.BaseURL+"/query", vals)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf(
			"Bad status code during Create(%s): %d, body: %s",
			command, resp.StatusCode, string(body),
		)
	}

	return nil
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

func writeURLFromConfig(cfg ClientConfig) string {
	params := url.Values{}
	params.Set("db", cfg.Database)
	if cfg.RetentionPolicy != "" {
		params.Set("rp", cfg.RetentionPolicy)
	}
	if cfg.Precision != "n" && cfg.Precision != "" {
		params.Set("precision", cfg.Precision)
	}
	if cfg.Consistency != "one" && cfg.Consistency != "" {
		params.Set("consistency", cfg.Consistency)
	}

	return cfg.BaseURL + "/write?" + params.Encode()
}
