package gpmc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type StorageQuota struct {
	UsageBytes       int64 `json:"usageBytes"`
	LimitBytes       int64 `json:"limitBytes"`
	PhotosUsageBytes int64 `json:"photosUsageBytes"`
	Unlimited        bool  `json:"unlimited"`
}

func (q StorageQuota) AvailableBytes() int64 {
	if q.Unlimited || q.LimitBytes <= 0 {
		return 0
	}
	if q.UsageBytes >= q.LimitBytes {
		return 0
	}
	return q.LimitBytes - q.UsageBytes
}

type driveTokenCache struct {
	mu     sync.Mutex
	token  string
	expiry time.Time
}

func (c *Client) GetStorageQuota(ctx context.Context) (StorageQuota, error) {
	if q, err := c.readPhotosStorageQuota(ctx); err == nil {
		return q, nil
	}
	return c.readDriveStorageQuota(ctx)
}

func (c *Client) readPhotosStorageQuota(ctx context.Context) (StorageQuota, error) {
	respBytes, err := c.doProto(ctx, "read-storage-quota", endpointReadStorageQuota, []byte{}, true, c.language)
	if err != nil {
		return StorageQuota{}, err
	}
	f1, ok := findFieldBytes(respBytes, 1)
	if !ok {
		return StorageQuota{}, fmt.Errorf("gpmc storage: missing quota payload")
	}
	var q StorageQuota
	if v, ok := findFieldVarint(f1, 1); ok {
		q.UsageBytes = int64(v)
	}
	if v, ok := findFieldVarint(f1, 2); ok {
		q.LimitBytes = int64(v)
	}
	if v, ok := findFieldVarint(f1, 3); ok {
		q.PhotosUsageBytes = int64(v)
	}
	if q.LimitBytes <= 0 || q.LimitBytes == ^int64(0) {
		q.Unlimited = true
		q.LimitBytes = 0
	}
	return q, nil
}

func (c *Client) readDriveStorageQuota(ctx context.Context) (StorageQuota, error) {
	tok, err := c.driveBearer(ctx)
	if err != nil {
		return StorageQuota{}, fmt.Errorf("gpmc storage: get drive token: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpointDriveAbout, nil)
	if err != nil {
		return StorageQuota{}, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpc.Do(req)
	if err != nil {
		return StorageQuota{}, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return StorageQuota{}, &APIError{Op: "drive-about", Status: resp.StatusCode, Body: string(body)}
	}

	var parsed struct {
		StorageQuota struct {
			Limit string `json:"limit"`
			Usage string `json:"usage"`
		} `json:"storageQuota"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return StorageQuota{}, fmt.Errorf("gpmc storage: decode json: %w", err)
	}
	q := StorageQuota{}
	if v, err := strconv.ParseInt(parsed.StorageQuota.Usage, 10, 64); err == nil {
		q.UsageBytes = v
	}
	if parsed.StorageQuota.Limit == "" {
		q.Unlimited = true
	} else if v, err := strconv.ParseInt(parsed.StorageQuota.Limit, 10, 64); err == nil {
		q.LimitBytes = v
	}
	return q, nil
}

func (c *Client) driveBearer(ctx context.Context) (string, error) {
	c.drive.mu.Lock()
	defer c.drive.mu.Unlock()
	if c.drive.token != "" && time.Now().Add(30*time.Second).Before(c.drive.expiry) {
		return c.drive.token, nil
	}
	tok, exp, err := c.fetchScopedToken(ctx, "oauth2:https://www.googleapis.com/auth/drive.appdata")
	if err != nil {
		return "", err
	}
	c.drive.token, c.drive.expiry = tok, exp
	return tok, nil
}

func (c *Client) fetchScopedToken(ctx context.Context, service string) (string, time.Time, error) {
	parsed, err := url.ParseQuery(c.authData)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("gpmc: parse auth_data: %w", err)
	}
	form := url.Values{}
	for _, k := range authFields {
		if k == "service" {
			continue
		}
		if v := parsed.Get(k); v != "" {
			form.Set(k, v)
		}
	}
	form.Set("service", service)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpointAuth, strings.NewReader(form.Encode()))
	if err != nil {
		return "", time.Time{}, err
	}
	req.Header.Set("Content-Type", contentTypeForm)
	req.Header.Set("User-Agent", authUA(c.profile))
	req.Header.Set("app", "com.google.android.apps.photos")
	if dev := parsed.Get("androidId"); dev != "" {
		req.Header.Set("device", dev)
	}

	resp, err := c.httpc.Do(req)
	if err != nil {
		return "", time.Time{}, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", time.Time{}, &AuthError{Status: resp.StatusCode, Body: string(body)}
	}
	kv := parseKVLines(string(body))
	tok := kv["Auth"]
	if tok == "" {
		return "", time.Time{}, &AuthError{Status: resp.StatusCode, Body: string(body)}
	}
	expSec, _ := strconv.ParseInt(kv["Expiry"], 10, 64)
	return tok, time.Unix(expSec, 0), nil
}
