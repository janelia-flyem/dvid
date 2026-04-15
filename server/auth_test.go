package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
)

func TestExtractDSGToken(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/repo/uuid/info", nil)
	req.AddCookie(&http.Cookie{Name: "dsg_token", Value: "cookie-token"})
	req.Header.Set("Authorization", "Bearer header-token")
	if got := extractDSGToken(req); got != "header-token" {
		t.Fatalf("expected header token precedence, got %q", got)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/repo/uuid/info", nil)
	req.AddCookie(&http.Cookie{Name: "dsg_token", Value: "cookie-token"})
	if got := extractDSGToken(req); got != "cookie-token" {
		t.Fatalf("expected cookie token fallback, got %q", got)
	}
}

func TestGetDSGUserUsesCache(t *testing.T) {
	origAuth := tc.Auth
	origClient := dsgHTTPClient
	defer func() {
		tc.Auth = origAuth
		dsgHTTPClient = origClient
		clearDSGUserCache()
	}()

	var requests int
	dsgHTTPClient = &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			requests++
			if got := r.Header.Get("Authorization"); got != "Bearer good-token" {
				t.Fatalf("expected bearer auth header, got %q", got)
			}
			body, err := json.Marshal(&dsgUserCache{
				Email:         "user@example.org",
				PermissionsV2: map[string][]string{"vnc": {"view"}},
			})
			if err != nil {
				t.Fatalf("could not encode DSG response: %v", err)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(bytes.NewReader(body)),
			}, nil
		}),
	}

	tc.Auth.DSGAddress = "https://auth.example.org"
	tc.Auth.DSGCacheTTL = 300
	clearDSGUserCache()

	user, err := getDSGUser("good-token")
	if err != nil {
		t.Fatalf("unexpected DSG auth error: %v", err)
	}
	if user.Email != "user@example.org" {
		t.Fatalf("unexpected user email %q", user.Email)
	}
	user, err = getDSGUser("good-token")
	if err != nil {
		t.Fatalf("unexpected cached DSG auth error: %v", err)
	}
	if user.Email != "user@example.org" {
		t.Fatalf("unexpected cached user email %q", user.Email)
	}
	if requests != 1 {
		t.Fatalf("expected one DSG request due to caching, got %d", requests)
	}
}

func TestGetDSGUserErrorStatuses(t *testing.T) {
	origAuth := tc.Auth
	origClient := dsgHTTPClient
	defer func() {
		tc.Auth = origAuth
		dsgHTTPClient = origClient
		clearDSGUserCache()
	}()

	tc.Auth.DSGAddress = "https://auth.example.org"

	if _, err := getDSGUser(""); err == nil {
		t.Fatalf("expected missing token error")
	} else {
		var authErr *authError
		if !errors.As(err, &authErr) || authErr.status != http.StatusUnauthorized {
			t.Fatalf("expected unauthorized auth error, got %v", err)
		}
	}

	dsgHTTPClient = &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusForbidden,
				Header:     make(http.Header),
				Body:       io.NopCloser(bytes.NewReader(nil)),
			}, nil
		}),
	}
	if _, err := getDSGUser("bad-token"); err == nil {
		t.Fatalf("expected invalid token error")
	} else {
		var authErr *authError
		if !errors.As(err, &authErr) || authErr.status != http.StatusUnauthorized {
			t.Fatalf("expected unauthorized auth error, got %v", err)
		}
	}

	dsgHTTPClient = &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return nil, io.EOF
		}),
	}
	if _, err := getDSGUser("network-fail"); err == nil {
		t.Fatalf("expected DSG upstream error")
	} else {
		var authErr *authError
		if !errors.As(err, &authErr) || authErr.status != http.StatusBadGateway {
			t.Fatalf("expected bad gateway auth error, got %v", err)
		}
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return fn(r)
}

func TestEffectiveEnforceInternal(t *testing.T) {
	origAuth := tc.Auth
	defer func() {
		tc.Auth = origAuth
	}()

	tc.Auth.Enforce = "dsg"
	tc.Auth.EnforceInternal = "none"
	tc.Auth.InternalCIDRs = []string{"10.0.0.0/8"}

	req := httptest.NewRequest(http.MethodGet, "/api/repo/uuid/info", nil)
	req.RemoteAddr = "10.2.3.4:5555"
	if got := effectiveEnforce(req); got != "none" {
		t.Fatalf("expected internal request to bypass auth, got %q", got)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/repo/uuid/info", nil)
	req.RemoteAddr = "35.1.2.3:5555"
	if got := effectiveEnforce(req); got != "dsg" {
		t.Fatalf("expected external request to use dsg auth, got %q", got)
	}
}

func TestDSGDatasetForRootUUID(t *testing.T) {
	origAuth := tc.Auth
	defer func() {
		tc.Auth = origAuth
	}()

	rootUUID := dvid.UUID("0123456789abcdef0123456789abcdef")
	tc.Auth.DatasetMap = map[string]string{string(rootUUID): "vnc"}

	got, err := dsgDatasetForRootUUID(rootUUID)
	if err != nil {
		t.Fatalf("unexpected dataset map error: %v", err)
	}
	if got != "vnc" {
		t.Fatalf("expected mapped dataset vnc, got %q", got)
	}

	tc.Auth.DatasetMap = map[string]string{}
	if _, err := dsgDatasetForRootUUID(rootUUID); err == nil {
		t.Fatalf("expected missing mapping error")
	} else {
		var authErr *authError
		if !errors.As(err, &authErr) || authErr.status != http.StatusForbidden {
			t.Fatalf("expected forbidden auth error, got %v", err)
		}
	}
}

func TestCachedDSGUserExpires(t *testing.T) {
	origAuth := tc.Auth
	defer func() {
		tc.Auth = origAuth
		clearDSGUserCache()
	}()

	tc.Auth.DSGCacheTTL = 1
	clearDSGUserCache()
	cacheDSGUser("token", &dsgUserCache{Email: "user@example.org"})
	if user := cachedDSGUserForToken("token"); user == nil {
		t.Fatalf("expected cached user before expiry")
	}

	authorizations.Lock()
	entry := authorizations.dsgUsers["token"]
	entry.fetchedAt = time.Now().Add(-2 * time.Second)
	authorizations.dsgUsers["token"] = entry
	authorizations.Unlock()

	if user := cachedDSGUserForToken("token"); user != nil {
		t.Fatalf("expected cached user to expire")
	}
}
