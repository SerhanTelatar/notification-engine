// Package provider calls the external notification provider over HTTP (contracts/provider.md).
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/platform/tracing"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// maxResponseBytes caps how much of a response body is read.
const maxResponseBytes = 64 << 10

// errorSnippetBytes caps how much of an error body is kept in last_error.
const errorSnippetBytes = 200

// minMaskLen: shorter values (a one-word content like "hi") aren't identifying, and masking them
// would mangle ordinary words in the error text.
const minMaskLen = 4

// ObserveFunc records provider latency (provider_request_duration_seconds).
type ObserveFunc func(ch domain.Channel, outcome domain.Outcome, d time.Duration)

// Client implements ports.Provider.
type Client struct {
	url     string
	timeout time.Duration
	http    *http.Client
	observe ObserveFunc
	now     func() time.Time
}

var _ ports.Provider = (*Client)(nil)

// NewClient returns a client for endpoint. Every call is bounded by timeout (FR-023), and
// redirects aren't followed, so a 3xx is classified as permanent.
func NewClient(endpoint string, timeout time.Duration, observe ObserveFunc, opts ...ClientOption) *Client {
	if observe == nil {
		observe = func(domain.Channel, domain.Outcome, time.Duration) {}
	}
	c := &Client{
		url:     endpoint,
		timeout: timeout,
		observe: observe,
		now:     time.Now,
		http: &http.Client{
			Timeout:       timeout,
			Transport:     newTransport(),
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// newTransport keeps enough idle connections for every concurrent consumer. The default of two
// per host makes most calls dial a new connection, which wastes time and adds variable latency
// between a call's rate-limit slot and its arrival at the provider.
func newTransport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone() //nolint:forcetypeassert // stdlib default
	t.MaxIdleConns = 512
	t.MaxIdleConnsPerHost = 256
	t.IdleConnTimeout = 90 * time.Second
	return t
}

type sendBody struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

type acceptedBody struct {
	MessageID string `json:"messageId"`
}

// Send posts one notification and classifies the response (research R9).
func (c *Client) Send(ctx context.Context, req ports.ProviderRequest) ports.ProviderResult {
	start := c.now()
	res := c.send(ctx, req)
	res.Latency = c.now().Sub(start)
	c.observe(req.Channel, res.Outcome, res.Latency)
	return res
}

func (c *Client) send(ctx context.Context, req ports.ProviderRequest) ports.ProviderResult {
	payload, err := json.Marshal(sendBody{To: req.To, Channel: string(req.Channel), Content: req.Content})
	if err != nil {
		return ports.ProviderResult{Outcome: domain.OutcomePermanent, Error: "encode request: " + err.Error()}
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(payload))
	if err != nil {
		return ports.ProviderResult{Outcome: domain.OutcomePermanent, Error: "build request: " + redact(err)}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Idempotency-Key", req.NotificationID)
	if req.CorrelationID != "" {
		httpReq.Header.Set("X-Correlation-ID", req.CorrelationID)
	}

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return ports.ProviderResult{Outcome: domain.Classify(0, err), Error: transportError(err, c.timeout)}
	}
	defer func() { _ = resp.Body.Close() }()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))

	status := resp.StatusCode
	res := ports.ProviderResult{Outcome: domain.Classify(status, nil), HTTPStatus: &status}
	if res.Outcome == domain.OutcomeSuccess {
		// A 2xx is accepted even if the body can't be parsed; the message id is then absent
		// (spec Edge Cases).
		var ok acceptedBody
		if readErr == nil && json.Unmarshal(body, &ok) == nil && ok.MessageID != "" {
			res.MessageID = &ok.MessageID
		}
		return res
	}
	res.RetryAfter = domain.ParseRetryAfter(resp.Header.Get("Retry-After"), c.now())
	res.Error = fmt.Sprintf("provider returned HTTP %d%s", status, snippet(body, req.To, req.Content))
	return res
}

// transportError describes a failed call without echoing the provider URL, which may hold a
// token (webhook.site) and would otherwise reach last_error and the API.
func transportError(err error, timeout time.Duration) string {
	var netErr net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &netErr) && netErr.Timeout()) {
		return fmt.Sprintf("provider timed out after %s", timeout)
	}
	return "provider request failed: " + redact(err)
}

func redact(err error) string {
	var uerr *url.Error
	if errors.As(err, &uerr) {
		return uerr.Err.Error()
	}
	return err.Error()
}

// snippet is a short, single-line excerpt of a provider error body for last_error and the logs.
// Providers often echo the request, so the recipient and content are masked first: neither may
// reach a log line (Constitution IV).
func snippet(body []byte, secrets ...string) string {
	s := string(body)
	for _, v := range secrets {
		if len(v) < minMaskLen {
			continue
		}
		s = strings.ReplaceAll(s, v, "[redacted]")
		if esc, err := json.Marshal(v); err == nil { // as echoed inside a JSON body
			s = strings.ReplaceAll(s, string(esc[1:len(esc)-1]), "[redacted]")
		}
	}
	s = strings.Join(strings.Fields(s), " ")
	if s == "" {
		return ""
	}
	if len(s) > errorSnippetBytes {
		s = s[:errorSnippetBytes] + "…"
	}
	return ": " + s
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithTracing records a client span per provider call ("provider send") and sends the W3C
// traceparent header, so a provider that traces joins the notification's trace (bonus, T117).
func WithTracing(t *tracing.Tracing) ClientOption {
	return func(c *Client) {
		if !t.Enabled() {
			return
		}
		c.http.Transport = otelhttp.NewTransport(c.http.Transport,
			otelhttp.WithTracerProvider(t.Provider()),
			otelhttp.WithPropagators(t.Propagator()),
			otelhttp.WithSpanNameFormatter(func(string, *http.Request) string { return "provider send" }),
		)
	}
}
