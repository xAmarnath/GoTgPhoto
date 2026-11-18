package gpmc

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
)

func TestAlbumProtobufModeEncoding(t *testing.T) {
	// Empty mediaKeys -> field 3 should be 2
	title := "Empty Album"
	keys := []string{}
	mode := int64(1)
	if len(keys) == 0 {
		mode = 2
	}
	body := encString(nil, 1, title)
	body = encInt(body, 2, 1234567890)
	body = encInt(body, 3, mode)

	// Field 3 wire tag is (3 << 3) | wireVarint(0) = 0x18
	// Value 2 is varint 0x02
	expectedTagVal := []byte{0x18, 0x02}
	if !bytes.Contains(body, expectedTagVal) {
		t.Fatalf("expected body to contain field 3 with value 2 (0x18 0x02)")
	}

	// Non-empty mediaKeys -> field 3 should be 1
	keys = []string{"AF1Qip..."}
	mode = int64(1)
	if len(keys) == 0 {
		mode = 2
	}
	body2 := encString(nil, 1, title)
	body2 = encInt(body2, 2, 1234567890)
	body2 = encInt(body2, 3, mode)

	expectedTagVal1 := []byte{0x18, 0x01}
	if !bytes.Contains(body2, expectedTagVal1) {
		t.Fatalf("expected body to contain field 3 with value 1 (0x18 0x01)")
	}
}

func TestLiveCreateEmptyAlbum(t *testing.T) {
	auth := os.Getenv("GP_AUTH_DATA")
	if auth == "" {
		// Read from .env if present
		b, err := os.ReadFile("../../.env")
		if err == nil {
			for _, line := range strings.Split(string(b), "\n") {
				line = strings.TrimSpace(line)
				if strings.HasPrefix(line, "GP_AUTH_DATA=") {
					auth = strings.TrimPrefix(line, "GP_AUTH_DATA=")
					auth = strings.Trim(auth, "\"' \r")
					break
				}
			}
		}
	}
	if auth == "" {
		t.Skip("skipping live test: GP_AUTH_DATA not set")
	}

	client, err := New(auth)
	if err != nil {
		t.Fatalf("New client error: %v", err)
	}

	ctx := context.Background()
	albumKey, err := client.CreateAlbum(ctx, "GPix Automated Empty Test", nil)
	if err != nil {
		t.Fatalf("CreateAlbum with empty mediaKeys failed: %v", err)
	}

	if !strings.HasPrefix(albumKey, "AF1Qip") {
		t.Fatalf("unexpected album key format: %s", albumKey)
	}
	t.Logf("Successfully created empty album upstream! Key: %s", albumKey)
}
