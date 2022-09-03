package write

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"sync"
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
	User            string
	Pass            string
	Precision       string
	Consistency     string
	TLSSkipVerify   bool

	Gzip bool

	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

type Client interface {
	Create(string) error
	Send([]byte) (latNs int64, statusCode int, body string, err error)

	Close() error
}

type client struct {
	url []byte

	cfg ClientConfig

	httpClient *fasthttp.Client
}

func NewClient(cfg ClientConfig) Client {
	var httpClient = &fasthttp.Client{}
	httpClient.WriteTimeout = cfg.WriteTimeout
	httpClient.ReadTimeout = cfg.ReadTimeout
	if cfg.TLSSkipVerify {
		httpClient.TLSConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}
	return &client{
		url:        []byte(writeURLFromConfig(cfg)),
		cfg:        cfg,
		httpClient: httpClient,
	}
}

func (c *client) Create(command string) error {
	if command == "" {
		command = "CREATE DATABASE " + c.cfg.Database
	}

	vals := url.Values{}
	vals.Set("q", command)
	u, err := url.Parse(c.cfg.BaseURL)
	if err != nil {
		return err
	}
	if c.cfg.User != "" && c.cfg.Pass != "" {
		u.User = url.UserPassword(c.cfg.User, c.cfg.Pass)
	}
	resp, err := http.PostForm(u.String()+"/query", vals)
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

func (c *client) Send(b []byte) (latNs int64, statusCode int, body string, err error) {
	req := fasthttp.AcquireRequest()
	req.Header.SetContentTypeBytes([]byte("text/plain"))
	req.Header.SetMethodBytes([]byte("POST"))
	req.Header.SetRequestURIBytes(c.url)
	if c.cfg.Gzip {
		req.Header.SetBytesKV([]byte("Content-Encoding"), []byte("gzip"))
	}
	req.Header.SetContentLength(len(b))
	req.SetBody(b)

	resp := fasthttp.AcquireResponse()
	start := time.Now()

	do := fasthttp.Do
	if c.httpClient != nil {
		do = c.httpClient.Do
	}

	err = do(req, resp)
	latNs = time.Since(start).Nanoseconds()
	statusCode = resp.StatusCode()

	// Save the body.
	if statusCode != http.StatusNoContent {
		body = string(resp.Body())
	}

	fasthttp.ReleaseResponse(resp)
	fasthttp.ReleaseRequest(req)

	return
}

func (c *client) Close() error {
	// Nothing to do.
	return nil
}

type fileClient struct {
	database string

	mu    sync.Mutex
	f     *os.File
	batch uint
}

func NewFileClient(path string, cfg ClientConfig) (Client, error) {
	c := &fileClient{}

	var err error
	c.f, err = os.Create(path)
	if err != nil {
		return nil, err
	}

	if _, err := c.f.WriteString("# " + writeURLFromConfig(cfg) + "\n"); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *fileClient) Create(command string) error {
	if command == "" {
		command = "CREATE DATABASE " + c.database
	}

	c.mu.Lock()
	_, err := fmt.Fprintf(c.f, "# create: %s\n\n", command)
	c.mu.Unlock()
	return err
}

func (c *fileClient) Send(b []byte) (latNs int64, statusCode int, body string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	statusCode = -1
	start := time.Now()
	defer func() {
		latNs = time.Since(start).Nanoseconds()
	}()

	c.batch++
	if _, err = fmt.Fprintf(c.f, "# Batch %d:\n", c.batch); err != nil {
		return
	}

	if _, err = c.f.Write(b); err != nil {
		return
	}

	if _, err = c.f.Write([]byte{'\n'}); err != nil {
		return
	}

	statusCode = 204
	return
}

func (c *fileClient) Close() error {
	return c.f.Close()
}

func writeURLFromConfig(cfg ClientConfig) string {
	params := url.Values{}
	params.Set("db", cfg.Database)
	if cfg.User != "" {
		params.Set("u", cfg.User)
	}
	if cfg.Pass != "" {
		params.Set("p", cfg.Pass)
	}
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
