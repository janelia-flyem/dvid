package server

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/zenazn/goji/web"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return fn(r)
}

func jsonHTTPResponse(status int, value interface{}) *http.Response {
	body, _ := json.Marshal(value)
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
}

func rawHTTPResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewBufferString(body)),
	}
}

func stringPointer(value string) *string { return &value }

func identityFor(email string, admin bool) dsgIdentity {
	return dsgIdentity{Email: stringPointer(email), Name: email, Admin: admin}
}

type fakeDSG struct {
	mu sync.Mutex

	identities          map[string]dsgIdentity
	authorize           func([]dsgAuthorizeEntry) (int, interface{})
	authorizeForService func(string, string, []dsgAuthorizeEntry) (int, interface{})

	identityCalls  int
	authorizeCalls int
	requests       []string
	services       []string
	returnURLs     []string
}

func (fake *fakeDSG) RoundTrip(r *http.Request) (*http.Response, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.requests = append(fake.requests, r.Method+" "+r.URL.Path)
	token := extractBearerToken(r.Header.Get("Authorization"))
	switch r.URL.Path {
	case "/api/dsg/v1/user":
		fake.identityCalls++
		identity, found := fake.identities[token]
		if !found {
			return jsonHTTPResponse(http.StatusUnauthorized, map[string]string{"error": "invalid token"}), nil
		}
		return jsonHTTPResponse(http.StatusOK, identity), nil
	case "/api/dsg/v1/authorize":
		fake.authorizeCalls++
		var request struct {
			Service   string              `json:"service"`
			ReturnURL string              `json:"return_url"`
			Entries   []dsgAuthorizeEntry `json:"entries"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			return rawHTTPResponse(http.StatusBadRequest, `{"error":"bad request"}`), nil
		}
		fake.services = append(fake.services, request.Service)
		fake.returnURLs = append(fake.returnURLs, request.ReturnURL)
		if fake.authorizeForService != nil {
			status, response := fake.authorizeForService(request.Service, request.ReturnURL, request.Entries)
			return jsonHTTPResponse(status, response), nil
		}
		if fake.authorize == nil {
			return jsonHTTPResponse(http.StatusOK, map[string]interface{}{"entries": []interface{}{}}), nil
		}
		status, response := fake.authorize(request.Entries)
		return jsonHTTPResponse(status, response), nil
	default:
		return jsonHTTPResponse(http.StatusNotFound, map[string]string{"error": "not found"}), nil
	}
}

func echoedDecision(entry dsgAuthorizeEntry, decision string, roles []string) dsgDecisionResponse {
	return dsgDecisionResponse{
		Name: stringPointer(entry.Name), Branch: stringPointer(entry.Branch),
		Version: stringPointer(entry.Version), Decision: decision, Roles: roles,
	}
}

func authorizeEnvelope(entries ...dsgDecisionResponse) map[string]interface{} {
	return map[string]interface{}{"entries": entries}
}

func withTestDSG(t *testing.T, fake http.RoundTripper) {
	t.Helper()
	origAuth := tc.Auth
	origClient := dsgHTTPClient
	t.Cleanup(func() {
		tc.Auth = origAuth
		dsgHTTPClient = origClient
		clearDSGCaches()
	})
	tc.Auth.Enforce = "dsg"
	tc.Auth.DSGAddress = "https://auth.example.org"
	tc.Auth.DSGService = "dvid"
	tc.Auth.DSGCacheTTL = 300
	dsgHTTPClient = &http.Client{Transport: fake}
	clearDSGCaches()
}

func requireAuthStatus(t *testing.T, err error, status int) {
	t.Helper()
	var authErr *authError
	if !errors.As(err, &authErr) || authErr.status != status {
		t.Fatalf("got error %v, want auth status %d", err, status)
	}
}

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

func TestGetDSGIdentityUsesCacheAndSupportsServiceAccounts(t *testing.T) {
	fake := &fakeDSG{identities: map[string]dsgIdentity{
		"human": identityFor("user@example.org", false),
		"sa":    {Email: nil, Name: "mesh-writer", ServiceAccount: true},
	}}
	withTestDSG(t, fake)

	identity, err := getDSGIdentity("human")
	if err != nil {
		t.Fatalf("unexpected identity error: %v", err)
	}
	if user, _ := identity.userName(); user != "user@example.org" {
		t.Fatalf("human attribution = %q", user)
	}
	if _, err := getDSGIdentity("human"); err != nil {
		t.Fatalf("unexpected cached identity error: %v", err)
	}
	identity, err = getDSGIdentity("sa")
	if err != nil {
		t.Fatalf("unexpected service-account identity error: %v", err)
	}
	if user, _ := identity.userName(); user != "mesh-writer" {
		t.Fatalf("service-account attribution = %q", user)
	}
	if fake.identityCalls != 2 {
		t.Fatalf("identity calls = %d, want 2", fake.identityCalls)
	}
}

func TestGetDSGIdentityErrorsAndExpiry(t *testing.T) {
	fake := &fakeDSG{identities: map[string]dsgIdentity{
		"empty": {},
		"good":  identityFor("user@example.org", false),
	}}
	withTestDSG(t, fake)

	_, err := getDSGIdentity("")
	requireAuthStatus(t, err, http.StatusUnauthorized)
	_, err = getDSGIdentity("bad")
	requireAuthStatus(t, err, http.StatusUnauthorized)
	_, err = getDSGIdentity("empty")
	requireAuthStatus(t, err, http.StatusUnauthorized)

	tc.Auth.DSGCacheTTL = 1
	if _, err := getDSGIdentity("good"); err != nil {
		t.Fatalf("could not cache good identity: %v", err)
	}
	authorizations.Lock()
	entry := authorizations.dsgUsers["good"]
	entry.fetchedAt = time.Now().Add(-2 * time.Second)
	authorizations.dsgUsers["good"] = entry
	authorizations.Unlock()
	if identity := cachedDSGIdentityForToken("good"); identity != nil {
		t.Fatalf("expected cached identity to expire")
	}
}

func TestGetDSGIdentityUpstreamErrors(t *testing.T) {
	withTestDSG(t, roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return nil, io.EOF
	}))
	_, err := getDSGIdentity("network-fail")
	requireAuthStatus(t, err, http.StatusBadGateway)

	dsgHTTPClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return rawHTTPResponse(http.StatusOK, "not-json"), nil
	})}
	_, err = getDSGIdentity("malformed")
	requireAuthStatus(t, err, http.StatusBadGateway)
}

func TestDSGDecisionCacheSeparatesPermissionsBothOrderings(t *testing.T) {
	entry := dsgAuthorizeEntry{Name: "root", Branch: "master", Version: "1"}
	fake := &fakeDSG{identities: map[string]dsgIdentity{}}
	fake.authorize = func(entries []dsgAuthorizeEntry) (int, interface{}) {
		responses := make([]dsgDecisionResponse, len(entries))
		for i, request := range entries {
			if request.Permission == "view" {
				responses[i] = echoedDecision(request, "allow", []string{"view"})
			} else {
				responses[i] = echoedDecision(request, "deny", nil)
			}
		}
		return http.StatusOK, authorizeEnvelope(responses...)
	}
	withTestDSG(t, fake)

	for _, first := range []string{"edit", "view"} {
		clearDSGCaches()
		fake.authorizeCalls = 0
		entry.Permission = first
		firstDecision, err := getDSGDecisions("token", []dsgAuthorizeEntry{entry})
		if err != nil {
			t.Fatalf("%s-first request failed: %v", first, err)
		}
		second := "view"
		if first == "view" {
			second = "edit"
		}
		entry.Permission = second
		secondDecision, err := getDSGDecisions("token", []dsgAuthorizeEntry{entry})
		if err != nil {
			t.Fatalf("%s-second request failed: %v", second, err)
		}
		if firstDecision[0].Decision == secondDecision[0].Decision {
			t.Fatalf("permission decisions were conflated: %s then %s", first, second)
		}
		if fake.authorizeCalls != 2 {
			t.Fatalf("%s/%s calls = %d, want 2", first, second, fake.authorizeCalls)
		}
	}
}

func TestDSGAuthorizeFailuresCacheNothing(t *testing.T) {
	entry := dsgAuthorizeEntry{Name: "root", Branch: "master", Version: "1", Permission: "view"}
	t.Run("invalid-token", func(t *testing.T) {
		withTestDSG(t, roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return rawHTTPResponse(http.StatusForbidden, "forbidden"), nil
		}))
		_, err := getDSGDecisions("token", []dsgAuthorizeEntry{entry})
		requireAuthStatus(t, err, http.StatusUnauthorized)
	})
	cases := map[string]roundTripFunc{
		"400": func(r *http.Request) (*http.Response, error) {
			return rawHTTPResponse(http.StatusBadRequest, `{"error":"entries must be a list"}`), nil
		},
		"500": func(r *http.Request) (*http.Response, error) {
			return rawHTTPResponse(http.StatusInternalServerError, "failed"), nil
		},
		"invalid-json": func(r *http.Request) (*http.Response, error) {
			return rawHTTPResponse(http.StatusOK, "not-json"), nil
		},
		"missing-entries": func(r *http.Request) (*http.Response, error) {
			return rawHTTPResponse(http.StatusOK, `{}`), nil
		},
		"non-list-entries": func(r *http.Request) (*http.Response, error) {
			return rawHTTPResponse(http.StatusOK, `{"entries": {}}`), nil
		},
	}
	for name, transport := range cases {
		t.Run(name, func(t *testing.T) {
			withTestDSG(t, transport)
			_, err := getDSGDecisions("token", []dsgAuthorizeEntry{entry})
			requireAuthStatus(t, err, http.StatusBadGateway)
			authorizations.RLock()
			cached := len(authorizations.dsgDecisions)
			authorizations.RUnlock()
			if cached != 0 {
				t.Fatalf("failure cached %d decisions", cached)
			}
		})
	}
}

func TestDSGAuthorizeCorrelationFailures(t *testing.T) {
	entry := dsgAuthorizeEntry{Name: "root", Branch: "master", Version: "1", Permission: "view"}
	t.Run("echo", func(t *testing.T) {
		fake := &fakeDSG{identities: map[string]dsgIdentity{}}
		fake.authorize = func(entries []dsgAuthorizeEntry) (int, interface{}) {
			response := echoedDecision(entries[0], "allow", []string{"view"})
			response.Version = stringPointer("2")
			return http.StatusOK, authorizeEnvelope(response)
		}
		withTestDSG(t, fake)
		_, err := getDSGDecisions("token", []dsgAuthorizeEntry{entry})
		requireAuthStatus(t, err, http.StatusBadGateway)
	})
	t.Run("length", func(t *testing.T) {
		fake := &fakeDSG{identities: map[string]dsgIdentity{}}
		fake.authorize = func(entries []dsgAuthorizeEntry) (int, interface{}) {
			return http.StatusOK, authorizeEnvelope()
		}
		withTestDSG(t, fake)
		_, err := getDSGDecisions("token", []dsgAuthorizeEntry{entry})
		requireAuthStatus(t, err, http.StatusBadGateway)
	})
}

func TestDSGTOSDecisionIsNeverCached(t *testing.T) {
	entry := dsgAuthorizeEntry{Name: "root", Branch: "master", Version: "1", Permission: "view"}
	state := "tos_required"
	fake := &fakeDSG{identities: map[string]dsgIdentity{}}
	fake.authorize = func(entries []dsgAuthorizeEntry) (int, interface{}) {
		response := echoedDecision(entries[0], state, []string{"view"})
		response.TOSURL = "https://dsg.example.org/tos/opaque"
		return http.StatusOK, authorizeEnvelope(response)
	}
	withTestDSG(t, fake)

	decisions, err := getDSGDecisions("token", []dsgAuthorizeEntry{entry})
	if err != nil || decisions[0].Decision != "tos_required" {
		t.Fatalf("first TOS decision = %+v, %v", decisions, err)
	}
	state = "allow"
	decisions, err = getDSGDecisions("token", []dsgAuthorizeEntry{entry})
	if err != nil || decisions[0].Decision != "allow" {
		t.Fatalf("immediate post-TOS decision = %+v, %v", decisions, err)
	}
	if fake.authorizeCalls != 2 {
		t.Fatalf("TOS flip calls = %d, want 2", fake.authorizeCalls)
	}
}

func TestDSGDecisionCacheSeparatesServicesAndTOSReturnURLs(t *testing.T) {
	entry := dsgAuthorizeEntry{Name: "root", Branch: "master", Version: "1", Permission: "view"}
	fake := &fakeDSG{identities: map[string]dsgIdentity{}}
	fake.authorizeForService = func(service, returnURL string, entries []dsgAuthorizeEntry) (int, interface{}) {
		response := echoedDecision(entries[0], "deny", nil)
		if service == "clio" {
			response.Decision = "allow"
			response.Roles = []string{"view"}
		}
		return http.StatusOK, authorizeEnvelope(response)
	}
	withTestDSG(t, fake)

	dvidDecision, err := getDSGDecisionsForService("token", "dvid", "", []dsgAuthorizeEntry{entry})
	if err != nil || dvidDecision[0].Decision != "deny" {
		t.Fatalf("dvid decision = %+v, %v", dvidDecision, err)
	}
	clioDecision, err := getDSGDecisionsForService("token", "clio", "", []dsgAuthorizeEntry{entry})
	if err != nil || clioDecision[0].Decision != "allow" {
		t.Fatalf("clio decision = %+v, %v", clioDecision, err)
	}
	if fake.authorizeCalls != 2 {
		t.Fatalf("service-separated calls = %d, want 2", fake.authorizeCalls)
	}
	if _, err := getDSGDecisionsForService("token", "dvid", "", []dsgAuthorizeEntry{entry}); err != nil {
		t.Fatal(err)
	}
	if _, err := getDSGDecisionsForService("token", "clio", "", []dsgAuthorizeEntry{entry}); err != nil {
		t.Fatal(err)
	}
	if fake.authorizeCalls != 2 {
		t.Fatalf("cached service-separated calls = %d, want 2", fake.authorizeCalls)
	}

	clearDSGCaches()
	fake.authorizeForService = func(service, returnURL string, entries []dsgAuthorizeEntry) (int, interface{}) {
		response := echoedDecision(entries[0], "tos_required", []string{"view"})
		response.TOSURL = returnURL + "/opaque"
		return http.StatusOK, authorizeEnvelope(response)
	}
	first, err := getDSGDecisionsForService("token", "clio", "https://one.example", []dsgAuthorizeEntry{entry})
	if err != nil {
		t.Fatal(err)
	}
	second, err := getDSGDecisionsForService("token", "clio", "https://two.example", []dsgAuthorizeEntry{entry})
	if err != nil {
		t.Fatal(err)
	}
	if first[0].TOSURL != "https://one.example/opaque" || second[0].TOSURL != "https://two.example/opaque" {
		t.Fatalf("TOS URLs crossed: %q, %q", first[0].TOSURL, second[0].TOSURL)
	}
	if fake.authorizeCalls != 4 {
		t.Fatalf("TOS requests should not cache: calls = %d, want 4", fake.authorizeCalls)
	}
}

func resignDVIDCapability(t *testing.T, token, secret string, mutate func(*dvidCapabilityClaims)) string {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		t.Fatalf("bad test capability %q", token)
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatal(err)
	}
	var claims dvidCapabilityClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		t.Fatal(err)
	}
	mutate(&claims)
	payload, err = json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	signature := dvidCapabilitySignature([]byte(encoded), secret)
	return encoded + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func TestDVIDCapabilityVerificationMatrixAndRotation(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()
	root, _ := datastore.NewTestRepo()
	other, _ := datastore.NewTestRepo()
	now := time.Unix(2_000_000_000, 0)
	t.Setenv(DVIDCapabilitySecretVarName, "secret-a")
	t.Setenv(DVIDCapabilityPreviousSecretVarName, "")

	editCapability, err := mintDVIDCapability(root, "edit", "editor@example.org", now)
	if err != nil {
		t.Fatal(err)
	}
	for _, permission := range []string{"view", "edit"} {
		principal, err := verifyDVIDCapability(editCapability, root, permission, now)
		if err != nil || principal != "editor@example.org" {
			t.Fatalf("edit capability for %s = %q, %v", permission, principal, err)
		}
	}
	viewCapability, err := mintDVIDCapability(root, "view", "viewer@example.org", now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifyDVIDCapability(viewCapability, root, "edit", now); err == nil {
		t.Fatal("view capability authorized a mutation")
	}
	if _, err := verifyDVIDCapability(editCapability, other, "view", now); err == nil {
		t.Fatal("capability authorized another node")
	}
	replacement := "A"
	if strings.HasSuffix(editCapability, replacement) {
		replacement = "B"
	}
	tampered := editCapability[:len(editCapability)-1] + replacement
	if _, err := verifyDVIDCapability(tampered, root, "view", now); err == nil {
		t.Fatal("tampered capability was accepted")
	}

	for name, mutate := range map[string]func(*dvidCapabilityClaims){
		"service": func(claims *dvidCapabilityClaims) { claims.Service = "dvid" },
		"root":    func(claims *dvidCapabilityClaims) { claims.Root = string(other) },
		"version": func(claims *dvidCapabilityClaims) { claims.Version++ },
	} {
		t.Run(name, func(t *testing.T) {
			changed := resignDVIDCapability(t, editCapability, "secret-a", mutate)
			if _, err := verifyDVIDCapability(changed, root, "view", now); err == nil {
				t.Fatalf("capability with wrong %s was accepted", name)
			}
		})
	}

	if _, err := verifyDVIDCapability(editCapability, root, "view", now.Add(dvidCapabilityTTL+dvidCapabilityClockSkew)); err != nil {
		t.Fatalf("capability at skew boundary failed: %v", err)
	}
	if _, err := verifyDVIDCapability(editCapability, root, "view", now.Add(dvidCapabilityTTL+dvidCapabilityClockSkew+time.Second)); err == nil {
		t.Fatal("expired capability beyond skew was accepted")
	}
	if _, err := verifyDVIDCapability(editCapability, root, "view", now.Add(-dvidCapabilityClockSkew)); err != nil {
		t.Fatalf("capability at future-clock skew boundary failed: %v", err)
	}
	if _, err := verifyDVIDCapability(editCapability, root, "view", now.Add(-dvidCapabilityClockSkew-time.Second)); err == nil {
		t.Fatal("not-yet-valid capability beyond skew was accepted")
	}

	t.Setenv(DVIDCapabilitySecretVarName, "secret-b")
	if _, err := verifyDVIDCapability(editCapability, root, "view", now); err == nil {
		t.Fatal("capability signed by unrelated secret was accepted")
	}
	t.Setenv(DVIDCapabilityPreviousSecretVarName, "secret-a")
	if principal, err := verifyDVIDCapability(editCapability, root, "view", now); err != nil || principal != "editor@example.org" {
		t.Fatalf("previous-secret verification = %q, %v", principal, err)
	}
}

func TestDecisionRolesMatrix(t *testing.T) {
	cases := []struct {
		roles      []string
		permission string
		allow      bool
	}{
		{[]string{"view"}, "view", true},
		{[]string{"view"}, "edit", false},
		{[]string{"edit"}, "edit", true},
		{[]string{"manage"}, "edit", true},
		{[]string{"admin"}, "edit", true},
		{[]string{"custom"}, "view", false},
	}
	for _, tc := range cases {
		if got := decisionRolesAllow(tc.roles, tc.permission); got != tc.allow {
			t.Errorf("roles %v permission %s = %t, want %t", tc.roles, tc.permission, got, tc.allow)
		}
	}
	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodOptions} {
		if permission := dsgPermissionForMethod(method); permission != "view" {
			t.Errorf("%s permission = %q, want view", method, permission)
		}
	}
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		if permission := dsgPermissionForMethod(method); permission != "edit" {
			t.Errorf("%s permission = %q, want edit", method, permission)
		}
	}
	for name, test := range map[string]struct {
		method     string
		isMutation bool
		permission string
	}{
		"immutable-post": {http.MethodPost, false, "view"},
		"mutation-post":  {http.MethodPost, true, "edit"},
		"get":            {http.MethodGet, false, "view"},
		"patch":          {http.MethodPatch, false, "edit"},
	} {
		t.Run(name, func(t *testing.T) {
			if got := dsgPermissionForDataRequest(test.method, test.isMutation); got != test.permission {
				t.Errorf("permission = %q, want %q", got, test.permission)
			}
		})
	}
}

func TestEvaluateDSGAnchorsDAGMatrix(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()

	root, _ := datastore.NewTestRepo()
	if err := datastore.Commit(root, "", nil); err != nil {
		t.Fatal(err)
	}
	left, err := datastore.NewVersion(root, "", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := datastore.Commit(left, "", nil); err != nil {
		t.Fatal(err)
	}
	right, err := datastore.NewVersion(root, "", "right", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := datastore.Commit(right, "", nil); err != nil {
		t.Fatal(err)
	}
	rootV, _ := datastore.VersionFromUUID(root)
	leftV, _ := datastore.VersionFromUUID(left)
	rightV, _ := datastore.VersionFromUUID(right)

	entry := dsgAuthorizeEntry{Name: string(root), Branch: "master", Version: strconv.FormatUint(uint64(rootV), 10), Permission: "view"}
	decision, err := evaluateDSGAnchors(entry, []dsgAnchor{{Branch: "master", Version: int64(leftV), Roles: []string{"view"}}})
	if err != nil || decision.Decision != "allow" {
		t.Fatalf("pre-fork history should be covered: %+v, %v", decision, err)
	}
	entry.Version = strconv.FormatUint(uint64(rightV), 10)
	decision, err = evaluateDSGAnchors(entry, []dsgAnchor{{Branch: "master", Version: int64(leftV), Roles: []string{"view"}}})
	if err != nil || decision.Decision != "deny" {
		t.Fatalf("sibling branch should be denied: %+v, %v", decision, err)
	}
	entry.Version = strconv.FormatUint(uint64(rootV), 10)
	decision, err = evaluateDSGAnchors(entry, []dsgAnchor{{Version: int64(^uint32(0)), Roles: []string{"view"}}})
	if err != nil || decision.Decision != "deny" {
		t.Fatalf("unknown anchor should be skipped: %+v, %v", decision, err)
	}
	entry.Version = strconv.FormatUint(uint64(leftV), 10)
	decision, err = evaluateDSGAnchors(entry, []dsgAnchor{{Version: int64(rootV), Roles: []string{"view"}}})
	if err != nil || decision.Decision != "deny" {
		t.Fatalf("ordinal pre-filter should deny newer request: %+v, %v", decision, err)
	}
}

// dsgUserRoundTrip is used by web tests for DSG-admin gates.
func dsgUserRoundTrip(t *testing.T, adminBearer string) roundTripFunc {
	t.Helper()
	return func(r *http.Request) (*http.Response, error) {
		identity := identityFor("user@example.org", r.Header.Get("Authorization") == adminBearer)
		return jsonHTTPResponse(http.StatusOK, identity), nil
	}
}

func TestAdminPrivilegedAndAuthorizeShortCircuit(t *testing.T) {
	origToken := adminToken
	t.Cleanup(func() { adminToken = origToken })
	adminToken = "secret"
	req := httptest.NewRequest(http.MethodPost, "/api/repos?admintoken=secret", nil)
	if !adminPrivileged(req) {
		t.Fatalf("valid admintoken should grant admin")
	}

	datastore.OpenTest()
	defer datastore.CloseTest()
	root, _ := datastore.NewTestRepo()
	fake := &fakeDSG{identities: map[string]dsgIdentity{"admin": identityFor("admin@example.org", true)}}
	fake.authorize = func(entries []dsgAuthorizeEntry) (int, interface{}) {
		t.Fatalf("admin short-circuit must not call authorize")
		return 0, nil
	}
	withTestDSG(t, fake)

	c := web.C{Env: map[interface{}]interface{}{"uuid": root}}
	handler := isAuthorized(&c, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	req = httptest.NewRequest(http.MethodPut, "/api/node/"+string(root)+"/data/key", nil)
	req.Header.Set("Authorization", "Bearer admin")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("DSG admin on unregistered repo got %d: %s", w.Code, w.Body.String())
	}
	if fake.authorizeCalls != 0 {
		t.Fatalf("admin short-circuit made %d authorize calls", fake.authorizeCalls)
	}
}

func TestAuthorizedIdentityOverridesQueryUserAndTOSMessage(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()
	root, _ := datastore.NewTestRepo()

	state := "allow"
	fake := &fakeDSG{identities: map[string]dsgIdentity{
		"sa":    {Name: "mesh-writer", ServiceAccount: true},
		"human": identityFor("human@example.org", false),
	}}
	fake.authorize = func(entries []dsgAuthorizeEntry) (int, interface{}) {
		response := echoedDecision(entries[0], state, []string{"edit"})
		response.TOSURL = "https://dsg.example.org/tos/opaque"
		return http.StatusOK, authorizeEnvelope(response)
	}
	withTestDSG(t, fake)

	c := web.C{Env: map[interface{}]interface{}{"uuid": root}}
	expectedUser := "mesh-writer"
	handler := isAuthorized(&c, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := requestUser(r); got != expectedUser {
			t.Errorf("authenticated user = %q, want %q", got, expectedUser)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodPost, "/api/node/"+string(root)+"/data/key?u=spoofed", nil)
	req.Header.Set("Authorization", "Bearer sa")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("authorized service account got %d: %s", w.Code, w.Body.String())
	}
	mutation := httpMutationRecord(req)
	if got := mutation["User"]; got != "mesh-writer" {
		t.Fatalf("HTTP mutation user = %v, want service-account slug", got)
	}
	versionedCtx := &datastore.VersionedCtx{}
	setVersionedCtxUser(versionedCtx, req)
	if versionedCtx.User != "mesh-writer" {
		t.Fatalf("VersionedCtx user = %q, want service-account slug", versionedCtx.User)
	}
	clearDSGCaches()
	expectedUser = "human@example.org"
	humanReq := httptest.NewRequest(http.MethodPost, "/api/node/"+string(root)+"/data/key?u=spoofed", nil)
	humanReq.Header.Set("Authorization", "Bearer human")
	humanW := httptest.NewRecorder()
	handler.ServeHTTP(humanW, humanReq)
	if humanW.Code != http.StatusNoContent {
		t.Fatalf("authorized human got %d: %s", humanW.Code, humanW.Body.String())
	}
	if got := httpMutationRecord(humanReq)["User"]; got != "human@example.org" {
		t.Fatalf("HTTP mutation user = %v, want human email", got)
	}

	clearDSGCaches()
	expectedUser = "human@example.org"
	state = "tos_required"
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, humanReq)
	if w.Code != http.StatusForbidden || !bytes.Contains(w.Body.Bytes(), []byte("https://dsg.example.org/tos/opaque")) {
		t.Fatalf("TOS response = %d %q", w.Code, w.Body.String())
	}
}

func TestClioAuthBrokerMintsCapabilityAndPreservesTOSURL(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()
	root, rootVersion := datastore.NewTestRepo()
	state := "allow"
	fake := &fakeDSG{identities: map[string]dsgIdentity{
		"viewer": identityFor("viewer@example.org", false),
		"admin":  identityFor("admin@example.org", true),
	}}
	fake.authorizeForService = func(service, returnURL string, entries []dsgAuthorizeEntry) (int, interface{}) {
		if service != dvidCapabilityService {
			t.Fatalf("broker service = %q", service)
		}
		entry := entries[0]
		if entry.Name != string(root) || entry.Branch != "master" ||
			entry.Version != strconv.FormatUint(uint64(rootVersion), 10) || entry.Permission != "view" {
			t.Fatalf("broker entry = %+v", entry)
		}
		response := echoedDecision(entry, state, []string{"view", "named-role"})
		if state == "tos_required" {
			response.TOSURL = returnURL + "?opaque=1"
		}
		return http.StatusOK, authorizeEnvelope(response)
	}
	withTestDSG(t, fake)
	t.Setenv(DVIDCapabilitySecretVarName, "broker-secret")
	t.Setenv(DVIDCapabilityPreviousSecretVarName, "")
	c := web.C{Env: map[interface{}]interface{}{"uuid": root}}

	requestBody := `{"permission":"view","return_url":"https://clio.example.org/return"}`
	req := httptest.NewRequest(http.MethodPost, "/api/auth/clio/"+string(root), strings.NewReader(requestBody))
	req.Header.Set("Authorization", "Bearer viewer")
	w := httptest.NewRecorder()
	clioAuthHandler(c, w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("broker allow = %d: %s", w.Code, w.Body.String())
	}
	var response clioAuthResponse
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.Decision != "allow" || response.Capability == "" || len(response.Roles) != 2 {
		t.Fatalf("broker allow response = %+v", response)
	}
	principal, err := verifyDVIDCapability(response.Capability, root, "view", time.Now())
	if err != nil || principal != "viewer@example.org" {
		t.Fatalf("minted capability principal = %q, %v", principal, err)
	}
	if len(fake.returnURLs) != 1 || fake.returnURLs[0] != "https://clio.example.org/return" {
		t.Fatalf("broker return URLs = %v", fake.returnURLs)
	}

	clearDSGCaches()
	state = "tos_required"
	req = httptest.NewRequest(http.MethodPost, "/api/auth/clio/"+string(root), strings.NewReader(requestBody))
	req.Header.Set("Authorization", "Bearer viewer")
	w = httptest.NewRecorder()
	clioAuthHandler(c, w, req)
	response = clioAuthResponse{}
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.Decision != "tos_required" || response.Capability != "" || response.TOSURL != "https://clio.example.org/return?opaque=1" {
		t.Fatalf("broker TOS response = %+v", response)
	}

	clearDSGCaches()
	state = "deny"
	req = httptest.NewRequest(http.MethodPost, "/api/auth/clio/"+string(root), strings.NewReader(requestBody))
	req.Header.Set("Authorization", "Bearer viewer")
	w = httptest.NewRecorder()
	clioAuthHandler(c, w, req)
	response = clioAuthResponse{}
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.Decision != "deny" || response.Capability != "" || response.TOSURL != "" {
		t.Fatalf("broker deny response = %+v", response)
	}

	clearDSGCaches()
	beforeAdmin := fake.authorizeCalls
	req = httptest.NewRequest(http.MethodPost, "/api/auth/clio/"+string(root), strings.NewReader(`{"permission":"edit"}`))
	req.Header.Set("Authorization", "Bearer admin")
	w = httptest.NewRecorder()
	clioAuthHandler(c, w, req)
	response = clioAuthResponse{}
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.Decision != "allow" || response.Capability == "" || fake.authorizeCalls != beforeAdmin {
		t.Fatalf("broker admin response = %+v, authorize calls %d -> %d", response, beforeAdmin, fake.authorizeCalls)
	}
	if _, err := verifyDVIDCapability(response.Capability, root, "edit", time.Now()); err != nil {
		t.Fatalf("admin edit capability failed: %v", err)
	}

	clearDSGCaches()
	req = httptest.NewRequest(http.MethodPost, "/api/auth/clio/"+string(root), strings.NewReader(requestBody))
	req.Header.Set("Authorization", "Bearer invalid")
	w = httptest.NewRecorder()
	clioAuthHandler(c, w, req)
	if w.Code != http.StatusUnauthorized || bytes.Contains(w.Body.Bytes(), []byte("capability")) {
		t.Fatalf("invalid-token broker response = %d: %s", w.Code, w.Body.String())
	}
}

func TestClioAuthBrokerConfigurationFailsClosed(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()
	root, _ := datastore.NewTestRepo()
	withTestDSG(t, &fakeDSG{identities: map[string]dsgIdentity{}})
	t.Setenv(DVIDCapabilitySecretVarName, "configured-secret")
	c := web.C{Env: map[interface{}]interface{}{"uuid": root}}

	request := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/auth/clio/"+string(root), strings.NewReader(`{"permission":"view"}`))
		req.Header.Set("Authorization", "Bearer user")
		w := httptest.NewRecorder()
		clioAuthHandler(c, w, req)
		return w
	}

	tc.Auth.Enforce = "none"
	if w := request(); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("broker without top-level DSG enforcement = %d: %s", w.Code, w.Body.String())
	}
	tc.Auth.Enforce = "dsg"
	tc.Auth.DSGAddress = ""
	if w := request(); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("broker without DSG address = %d: %s", w.Code, w.Body.String())
	}
	tc.Auth.DSGAddress = "https://auth.example.org"
	t.Setenv(DVIDCapabilitySecretVarName, "")
	if w := request(); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("broker without capability secret = %d: %s", w.Code, w.Body.String())
	}

	tc.Auth.EnforceInternal = "none"
	handler := isAuthorized(&c, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/node/"+string(root)+"/data/key", nil)
	req.Header.Set(internalHeader, "true")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("missing capability secret affected no-capability legacy request: %d: %s", w.Code, w.Body.String())
	}
}

func TestDVIDCapabilityPrecedesPublicAndInternalBypasses(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()
	root, rootVersion := datastore.NewTestRepo()
	if err := datastore.Commit(root, "", nil); err != nil {
		t.Fatal(err)
	}
	if err := datastore.SetPublicVersionsFromSeeds([]dvid.VersionID{rootVersion}); err != nil {
		t.Fatal(err)
	}
	withTestDSG(t, &fakeDSG{identities: map[string]dsgIdentity{}})
	tc.Auth.EnforceInternal = "none"
	t.Setenv(DVIDCapabilitySecretVarName, "precedence-secret")
	t.Setenv(DVIDCapabilityPreviousSecretVarName, "")
	capability, err := mintDVIDCapability(root, "view", "capability-user@example.org", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	c := web.C{Env: map[interface{}]interface{}{"uuid": root}}

	run := func(name, authorization string, internal bool, wantStatus int, wantUser string) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			handler := isAuthorized(&c, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := requestUser(r); got != wantUser {
					t.Errorf("request user = %q, want %q", got, wantUser)
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			req := httptest.NewRequest(http.MethodGet, "/api/node/"+string(root)+"/data/key?u=query-user", nil)
			if authorization != "" {
				req.Header.Set("Authorization", authorization)
			}
			if internal {
				req.Header.Set(internalHeader, "true")
			}
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
			if w.Code != wantStatus {
				t.Fatalf("status = %d, want %d: %s", w.Code, wantStatus, w.Body.String())
			}
		})
	}

	run("valid-public-internal", dvidCapabilityScheme+" "+capability, true, http.StatusNoContent, "capability-user@example.org")
	run("invalid-public-internal", dvidCapabilityScheme+" tampered", true, http.StatusUnauthorized, "")
	run("no-capability-public", "", false, http.StatusNoContent, "query-user")
	run("no-capability-internal", "", true, http.StatusNoContent, "query-user")
}

func TestDVIDCapabilityReplayPreservesMutationPrincipal(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()
	root, _ := datastore.NewTestRepo()
	withTestDSG(t, &fakeDSG{identities: map[string]dsgIdentity{}})
	t.Setenv(DVIDCapabilitySecretVarName, "replay-secret")
	capability, err := mintDVIDCapability(root, "edit", "original@example.org", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	c := web.C{Env: map[interface{}]interface{}{"uuid": root}}
	handler := isAuthorized(&c, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := requestUser(r); got != "original@example.org" {
			t.Errorf("request user = %q", got)
		}
		if got := httpMutationRecord(r)["User"]; got != "original@example.org" {
			t.Errorf("mutation user = %v", got)
		}
		ctx := &datastore.VersionedCtx{}
		setVersionedCtxUser(ctx, r)
		if ctx.User != "original@example.org" {
			t.Errorf("VersionedCtx user = %q", ctx.User)
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	for replay := 0; replay < 2; replay++ {
		req := httptest.NewRequest(http.MethodPost, "/api/node/"+string(root)+"/data/key?u=spoofed", nil)
		req.Header.Set("Authorization", dvidCapabilityScheme+" "+capability)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != http.StatusNoContent {
			t.Fatalf("replay %d status = %d: %s", replay, w.Code, w.Body.String())
		}
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
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/repo/uuid/info", nil)
	req.Header.Set(internalHeader, "true")
	req.RemoteAddr = "10.2.3.4:5555"
	if got := effectiveEnforce(req); got != "none" {
		t.Fatalf("trusted internal request enforce = %q", got)
	}
	req.RemoteAddr = "35.1.2.3:5555"
	if got := effectiveEnforce(req); got != "dsg" {
		t.Fatalf("external request enforce = %q", got)
	}
}

func TestRequestAccessScopeUsesRootProbe(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()
	root, _ := datastore.NewTestRepo()
	if err := datastore.Commit(root, "", nil); err != nil {
		t.Fatal(err)
	}
	anchor, err := datastore.NewVersion(root, "", "release", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := datastore.Commit(anchor, "", nil); err != nil {
		t.Fatal(err)
	}
	anchorV, _ := datastore.VersionFromUUID(anchor)

	decision := "service_eval"
	fake := &fakeDSG{identities: map[string]dsgIdentity{"viewer": identityFor("viewer@example.org", false)}}
	fake.authorize = func(entries []dsgAuthorizeEntry) (int, interface{}) {
		response := echoedDecision(entries[0], decision, []string{"view"})
		if decision == "service_eval" {
			response.Anchors = []dsgAnchor{{Branch: "release", Version: int64(anchorV), Roles: []string{"view"}}}
		}
		return http.StatusOK, authorizeEnvelope(response)
	}
	withTestDSG(t, fake)

	c := web.C{Env: map[interface{}]interface{}{"adminPriv": false}}
	req := httptest.NewRequest(http.MethodGet, "/api/repo/"+string(root)+"/info", nil)
	req.Header.Set("Authorization", "Bearer viewer")
	scope := requestAccessScope(c, req)
	visibility, err := scope.repoVisibility(root, http.MethodGet)
	if err != nil || visibility != datastore.RepoFullView {
		t.Fatalf("version-grain root probe visibility = %v, %v", visibility, err)
	}
	if fake.authorizeCalls != 1 {
		t.Fatalf("root probe authorize calls = %d, want 1", fake.authorizeCalls)
	}

	clearDSGCaches()
	decision = "deny"
	scope = requestAccessScope(c, req)
	visibility, err = scope.repoVisibility(root, http.MethodGet)
	if err != nil || visibility != datastore.RepoPublicOnly {
		t.Fatalf("denied root visibility = %v, %v", visibility, err)
	}

	clearDSGCaches()
	decision = "tos_required"
	scope = requestAccessScope(c, req)
	visibility, err = scope.repoVisibility(root, http.MethodGet)
	if err != nil || visibility != datastore.RepoPublicOnly {
		t.Fatalf("TOS-pending root visibility = %v, %v", visibility, err)
	}
}

func TestRefreshDSGPublicVersionsSeedsCommittedClosure(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()
	root, rootV := datastore.NewTestRepo()
	if err := datastore.Commit(root, "", nil); err != nil {
		t.Fatal(err)
	}
	publicHead, err := datastore.NewVersion(root, "", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := datastore.Commit(publicHead, "", nil); err != nil {
		t.Fatal(err)
	}
	publicV, _ := datastore.VersionFromUUID(publicHead)
	unlocked, err := datastore.NewVersion(publicHead, "", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	unlockedV, _ := datastore.VersionFromUUID(unlocked)
	otherRoot, _ := datastore.NewTestRepo()
	if err := datastore.Commit(otherRoot, "", nil); err != nil {
		t.Fatal(err)
	}
	otherV, _ := datastore.VersionFromUUID(otherRoot)

	t.Setenv(DSGTokenVarName, "seed-token")
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if got := r.Header.Get("Authorization"); got != "Bearer seed-token" {
			t.Fatalf("metadata Authorization = %q", got)
		}
		if got := r.URL.Query().Get("service"); got != "dvid" {
			t.Fatalf("metadata service = %q", got)
		}
		switch r.URL.Path {
		case "/api/dsg/v1/datasets":
			publicKey := "has_" + "public" + "_versions"
			return jsonHTTPResponse(http.StatusOK, []map[string]interface{}{
				{"name": string(root), publicKey: true},
				{"name": "not-a-local-root", publicKey: true},
			}), nil
		case "/api/dsg/v1/datasets/" + string(root) + "/versions":
			nullOrdinal := (*int64)(nil)
			return jsonHTTPResponse(http.StatusOK, []dsgVersionMetadata{
				{Ordinal: int64Pointer(int64(publicV)), IsPublic: true},
				{Ordinal: int64Pointer(int64(unlockedV)), IsPublic: true},
				{Ordinal: nullOrdinal, IsPublic: true},
				{Ordinal: int64Pointer(int64(otherV)), IsPublic: true},
				{Ordinal: int64Pointer(int64(rootV)), IsPublic: false},
			}), nil
		default:
			return jsonHTTPResponse(http.StatusNotFound, map[string]string{"error": "not found"}), nil
		}
	})
	withTestDSG(t, transport)

	if err := refreshDSGPublicVersions(); err != nil {
		t.Fatalf("public metadata refresh failed: %v", err)
	}
	if !datastore.IsPublic(root) || !datastore.IsPublic(publicHead) {
		t.Fatalf("public seed did not publish release closure")
	}
	if datastore.IsPublic(unlocked) || datastore.IsPublic(otherRoot) {
		t.Fatalf("public seed included unlocked or cross-repo version")
	}
}

func int64Pointer(value int64) *int64 { return &value }

func TestDSGPublicVersionSeedingStartupFailsClosedRefreshKeepsLastGood(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()
	root, rootV := datastore.NewTestRepo()
	if err := datastore.Commit(root, "", nil); err != nil {
		t.Fatal(err)
	}
	if err := datastore.SetPublicVersionsFromSeeds([]dvid.VersionID{rootV}); err != nil {
		t.Fatal(err)
	}
	t.Setenv(DSGTokenVarName, "seed-token")
	withTestDSG(t, roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return nil, io.EOF
	}))
	tc.Auth.DSGPublicRefresh = 0

	if err := refreshDSGPublicVersions(); err == nil {
		t.Fatalf("expected periodic refresh failure")
	}
	if !datastore.IsPublic(root) {
		t.Fatalf("periodic refresh failure replaced last-good set")
	}
	if err := StartDSGPublicVersionSeeding(); err != nil {
		t.Fatalf("startup fail-closed returned error: %v", err)
	}
	if datastore.IsPublic(root) {
		t.Fatalf("startup refresh failure did not publish empty set")
	}
}

func TestValidateAuthModesRejectsRetiredKeys(t *testing.T) {
	origAuth := tc.Auth
	defer func() { tc.Auth = origAuth }()
	tc.Auth = authConfig{Enforce: "dsg", DatasetMap: map[string]string{"root": "dataset"}}
	if err := validateAuthModes(); err == nil || !bytes.Contains([]byte(err.Error()), []byte("is retired")) {
		t.Fatalf("dataset map tombstone error = %v", err)
	}
	tc.Auth = authConfig{Enforce: "dsg", PublicVersions: []string{"uuid"}}
	if err := validateAuthModes(); err == nil || !bytes.Contains([]byte(err.Error()), []byte("is retired")) {
		t.Fatalf("public versions tombstone error = %v", err)
	}
}
