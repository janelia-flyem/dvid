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

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/zenazn/goji/web"
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

// dsgUserRoundTrip returns a DSG auth service mock that reports a user as
// admin iff the request bears the given Authorization header value.
func dsgUserRoundTrip(t *testing.T, adminBearer string) roundTripFunc {
	return func(r *http.Request) (*http.Response, error) {
		body, err := json.Marshal(&dsgUserCache{
			Email: "user@example.org",
			Admin: r.Header.Get("Authorization") == adminBearer,
		})
		if err != nil {
			t.Fatalf("could not encode DSG response: %v", err)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(bytes.NewReader(body)),
		}, nil
	}
}

func TestAdminPrivileged(t *testing.T) {
	origAuth := tc.Auth
	origClient := dsgHTTPClient
	origToken := adminToken
	defer func() {
		tc.Auth = origAuth
		dsgHTTPClient = origClient
		adminToken = origToken
		clearDSGUserCache()
	}()
	clearDSGUserCache()

	// With no admintoken configured and no DSG mode, nothing grants admin,
	// even though enforce=none makes requestAccessScope().full true for
	// every request.
	adminToken = ""
	tc.Auth = authConfig{}
	req := httptest.NewRequest(http.MethodPost, "/api/repos", nil)
	if adminPrivileged(req) {
		t.Fatalf("expected no admin privilege without credentials")
	}
	req = httptest.NewRequest(http.MethodPost, "/api/repos?admintoken=", nil)
	if adminPrivileged(req) {
		t.Fatalf("expected empty admintoken to never grant admin")
	}

	// A valid admintoken grants admin; a wrong one does not.
	adminToken = "secret"
	req = httptest.NewRequest(http.MethodPost, "/api/repos?admintoken=secret", nil)
	if !adminPrivileged(req) {
		t.Fatalf("expected valid admintoken to grant admin")
	}
	req = httptest.NewRequest(http.MethodPost, "/api/repos?admintoken=wrong", nil)
	if adminPrivileged(req) {
		t.Fatalf("expected wrong admintoken to be rejected")
	}

	// A DSG admin grants admin in dsg mode; a non-admin DSG user does not.
	adminToken = ""
	tc.Auth.Enforce = "dsg"
	tc.Auth.DSGAddress = "https://auth.example.org"
	tc.Auth.DSGCacheTTL = 300
	dsgHTTPClient = &http.Client{Transport: dsgUserRoundTrip(t, "Bearer admin-token")}
	req = httptest.NewRequest(http.MethodPost, "/api/repos", nil)
	req.Header.Set("Authorization", "Bearer admin-token")
	if !adminPrivileged(req) {
		t.Fatalf("expected DSG admin to be granted admin privilege")
	}
	req = httptest.NewRequest(http.MethodPost, "/api/repos", nil)
	req.Header.Set("Authorization", "Bearer user-token")
	if adminPrivileged(req) {
		t.Fatalf("expected non-admin DSG user to be denied admin privilege")
	}

	// Outside DSG modes, bearer tokens never trigger DSG lookups.
	clearDSGUserCache()
	tc.Auth.Enforce = "token"
	tc.Auth.EnforceInternal = ""
	dsgHTTPClient = &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			t.Fatalf("DSG service should not be contacted outside dsg modes")
			return nil, nil
		}),
	}
	req = httptest.NewRequest(http.MethodPost, "/api/repos", nil)
	req.Header.Set("Authorization", "Bearer admin-token")
	if adminPrivileged(req) {
		t.Fatalf("expected no DSG admin grant outside dsg modes")
	}
}

func TestEffectiveEnforceInternal(t *testing.T) {
	origAuth := tc.Auth
	origTrustedProxyNets := trustedProxyNets
	defer func() {
		tc.Auth = origAuth
		trustedProxyNets = origTrustedProxyNets
	}()

	tc.Auth.Enforce = "dsg"
	tc.Auth.EnforceInternal = "none"
	tc.Auth.TrustedProxies = []string{"10.0.0.0/8"}
	if err := configureTrustedProxies(); err != nil {
		t.Fatalf("could not configure trusted proxies: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/repo/uuid/info", nil)
	req.Header.Set(internalHeader, "true")
	req.RemoteAddr = "10.2.3.4:5555"
	if got := effectiveEnforce(req); got != "none" {
		t.Fatalf("expected internal request to bypass auth, got %q", got)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/repo/uuid/info", nil)
	req.RemoteAddr = "35.1.2.3:5555"
	req.Header.Set(internalHeader, "true")
	if got := effectiveEnforce(req); got != "dsg" {
		t.Fatalf("expected external request to use dsg auth, got %q", got)
	}

	tc.Auth.TrustedProxies = nil
	if err := configureTrustedProxies(); err != nil {
		t.Fatalf("could not clear trusted proxies: %v", err)
	}
	req = httptest.NewRequest(http.MethodGet, "/api/repo/uuid/info", nil)
	req.Header.Set(internalHeader, "true")
	req.RemoteAddr = "35.1.2.3:5555"
	if got := effectiveEnforce(req); got != "none" {
		t.Fatalf("expected header to be trusted without trusted_proxies, got %q", got)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/repo/uuid/info", nil)
	req.RemoteAddr = "10.2.3.4:5555"
	if got := effectiveEnforce(req); got != "dsg" {
		t.Fatalf("expected request without internal header to use dsg auth, got %q", got)
	}
}

func TestRequestAccessScope(t *testing.T) {
	origAuth := tc.Auth
	origClient := dsgHTTPClient
	origTrustedProxyNets := trustedProxyNets
	defer func() {
		tc.Auth = origAuth
		dsgHTTPClient = origClient
		trustedProxyNets = origTrustedProxyNets
		clearDSGUserCache()
	}()

	rootUUID := dvid.UUID("0123456789abcdef0123456789abcdef")
	tc.Auth.Enforce = "dsg"
	tc.Auth.EnforceInternal = "dsg"
	tc.Auth.DSGAddress = "https://auth.example.org"
	tc.Auth.DSGCacheTTL = 300
	tc.Auth.DatasetMap = map[string]string{string(rootUUID): "vnc"}
	tc.Auth.TrustedProxies = nil
	if err := configureTrustedProxies(); err != nil {
		t.Fatalf("could not configure trusted proxies: %v", err)
	}
	clearDSGUserCache()

	c := web.C{Env: map[interface{}]interface{}{"adminPriv": false}}
	req := httptest.NewRequest(http.MethodGet, "/api/repo/uuid/info", nil)
	req.Header.Set(internalHeader, "true")
	scope := requestAccessScope(c, req)
	if scope.full || scope.user != nil {
		t.Fatalf("internal request with enforce_internal=dsg and no token should be public-only: %+v", scope)
	}
	if got := scope.repoVisibility(rootUUID, http.MethodGet); got != datastore.RepoPublicOnly {
		t.Fatalf("expected public-only visibility without token, got %v", got)
	}

	tc.Auth.EnforceInternal = "none"
	scope = requestAccessScope(c, req)
	if !scope.full {
		t.Fatalf("internal request with enforce_internal=none should get full scope")
	}

	tc.Auth.EnforceInternal = "dsg"
	c = web.C{Env: map[interface{}]interface{}{"adminPriv": true}}
	scope = requestAccessScope(c, req)
	if !scope.full {
		t.Fatalf("admintoken should grant full scope")
	}

	c = web.C{Env: map[interface{}]interface{}{"adminPriv": false}}
	dsgHTTPClient = &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			body, err := json.Marshal(&dsgUserCache{
				Email:         "viewer@example.org",
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
	req = httptest.NewRequest(http.MethodGet, "/api/repo/uuid/info", nil)
	req.Header.Set("Authorization", "Bearer viewer-token")
	scope = requestAccessScope(c, req)
	if scope.full || scope.user == nil {
		t.Fatalf("non-admin DSG viewer should be scoped, not full: %+v", scope)
	}
	if got := scope.repoVisibility(rootUUID, http.MethodGet); got != datastore.RepoFullView {
		t.Fatalf("viewer should get full repo metadata for GET, got %v", got)
	}
	if got := scope.repoVisibility(rootUUID, http.MethodPost); got != datastore.RepoPublicOnly {
		t.Fatalf("viewer should not get full repo metadata for write intent, got %v", got)
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
