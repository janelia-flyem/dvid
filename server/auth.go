package server

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	jwt "github.com/golang-jwt/jwt/v4"
	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/zenazn/goji/web"
	"google.golang.org/api/oauth2/v2"
)

var (
	authorizations   authData
	jwtSecretKey     string
	httpClient       = &http.Client{}
	dsgHTTPClient    = &http.Client{Timeout: 10 * time.Second}
	trustedProxyNets []*net.IPNet
)

// SecretKeyVarName is the environment variable holding the secret key for JWT support.
const SecretKeyVarName = "DVID_JWT_SECRET_KEY"

// DSGTokenVarName holds DVID's own DSG credential for public metadata seeding.
const DSGTokenVarName = "DVID_DSG_TOKEN"

// DVIDCapabilitySecretVarName holds the active secret for Clio capability signing.
const DVIDCapabilitySecretVarName = "DVID_CAPABILITY_SECRET"

// DVIDCapabilityPreviousSecretVarName optionally holds the previous signing secret during rotation.
const DVIDCapabilityPreviousSecretVarName = "DVID_CAPABILITY_PREVIOUS_SECRET"

const (
	internalHeader              = "X-DVID-Internal"
	dvidCapabilityScheme        = "DVID-Capability"
	dvidCapabilityService       = "clio"
	dvidCapabilityFormatVersion = 1
	dvidCapabilityNonceBytes    = 16
	dvidCapabilityTTL           = 60 * time.Second
	dvidCapabilityClockSkew     = 5 * time.Second
)

func init() {
	jwtSecretKey = os.Getenv(SecretKeyVarName)
}

// authData holds legacy auth-file users and DSG-native identity/decision caches.
type authData struct {
	sync.RWMutex
	users        map[string]string
	dsgUsers     map[string]cachedDSGIdentity
	dsgDecisions map[dsgDecisionCacheKey]cachedDSGDecision
}

type cachedDSGIdentity struct {
	identity  *dsgIdentity
	fetchedAt time.Time
}

type dsgIdentity struct {
	ID             int64   `json:"id"`
	Email          *string `json:"email"`
	Name           string  `json:"name"`
	Admin          bool    `json:"admin"`
	ServiceAccount bool    `json:"service_account"`
}

func (identity *dsgIdentity) userName() (string, error) {
	if identity != nil && identity.Email != nil && *identity.Email != "" {
		return *identity.Email, nil
	}
	if identity != nil && identity.Name != "" {
		return identity.Name, nil
	}
	return "", &authError{
		status:  http.StatusUnauthorized,
		message: "malformed DSG identity: email and name are both empty",
	}
}

type dsgDecisionCacheKey struct {
	Token      string
	Service    string
	Name       string
	Branch     string
	Version    string
	Permission string
}

type cachedDSGDecision struct {
	decision  dsgDecision
	fetchedAt time.Time
}

type dsgAuthorizeEntry struct {
	Name       string `json:"name"`
	Branch     string `json:"branch"`
	Version    string `json:"version"`
	Permission string `json:"permission"`
}

type dsgDecision struct {
	Decision string
	Roles    []string
	TOSURL   string
}

type dsgDecisionResponse struct {
	Name     *string     `json:"name"`
	Branch   *string     `json:"branch"`
	Version  *string     `json:"version"`
	Decision string      `json:"decision"`
	Roles    []string    `json:"roles"`
	TOSURL   string      `json:"tos_url"`
	Anchors  []dsgAnchor `json:"anchors"`
}

type dsgAnchor struct {
	Branch  string   `json:"branch"`
	Version int64    `json:"version"`
	Roles   []string `json:"roles"`
}

type authError struct {
	status  int
	message string
}

type dvidCapabilityClaims struct {
	FormatVersion int    `json:"v"`
	Service       string `json:"svc"`
	Node          string `json:"node"`
	Root          string `json:"root"`
	Version       uint64 `json:"version"`
	Permission    string `json:"permission"`
	Principal     string `json:"principal"`
	IssuedAt      int64  `json:"iat"`
	ExpiresAt     int64  `json:"exp"`
	Nonce         string `json:"nonce"`
}

type clioAuthRequest struct {
	Permission string `json:"permission"`
	ReturnURL  string `json:"return_url,omitempty"`
}

type clioAuthResponse struct {
	Decision   string   `json:"decision"`
	Roles      []string `json:"roles,omitempty"`
	Capability string   `json:"capability,omitempty"`
	TOSURL     string   `json:"tos_url,omitempty"`
}

func (e *authError) Error() string {
	return e.message
}

func (auth *authData) initialize() error {
	if err := auth.loadAuthFile(); err != nil {
		return err
	}
	if err := validateAuthModes(); err != nil {
		return err
	}
	if err := configureTrustedProxies(); err != nil {
		return err
	}
	warnDeprecatedAuthMode(authMode())
	if tc.Auth.EnforceInternal != "" {
		warnDeprecatedAuthMode(strings.ToLower(tc.Auth.EnforceInternal))
	}
	auth.Lock()
	auth.dsgUsers = make(map[string]cachedDSGIdentity)
	auth.dsgDecisions = make(map[dsgDecisionCacheKey]cachedDSGDecision)
	auth.Unlock()
	return nil
}

// StartDSGPublicVersionSeeding seeds public releases after datastore.Initialize
// and starts the configured refresh loop. Startup failures publish an empty set;
// later refresh failures retain last-good.
func StartDSGPublicVersionSeeding() error {
	if !dsgPublicSeedingEnabled() {
		return datastore.SetPublicVersionsFromSeeds(nil)
	}
	if err := refreshDSGPublicVersions(); err != nil {
		if os.Getenv(DSGTokenVarName) == "" {
			dvid.Warningf("unable to seed public versions from DSG; failing closed: %v\n", err)
		} else {
			dvid.Errorf("unable to seed public versions from DSG; failing closed: %v\n", err)
		}
		if clearErr := datastore.SetPublicVersionsFromSeeds(nil); clearErr != nil {
			return clearErr
		}
	}
	if tc.Auth.DSGPublicRefresh > 0 {
		interval := time.Duration(tc.Auth.DSGPublicRefresh) * time.Second
		go func() {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for range ticker.C {
				if err := refreshDSGPublicVersions(); err != nil {
					dvid.Warningf("unable to refresh public versions from DSG; keeping last-good set: %v\n", err)
				}
			}
		}()
	}
	return nil
}

func dsgPublicSeedingEnabled() bool {
	return tc.Auth.DSGAddress != "" &&
		(authMode() == "dsg" || strings.ToLower(tc.Auth.EnforceInternal) == "dsg")
}

type dsgDatasetMetadata struct {
	Name              string `json:"name"`
	HasPublicVersions bool   `json:"-"`
}

func (metadata *dsgDatasetMetadata) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	if err := json.Unmarshal(fields["name"], &metadata.Name); err != nil {
		return err
	}
	publicKey := "has_" + "public" + "_versions"
	if value, found := fields[publicKey]; found {
		if err := json.Unmarshal(value, &metadata.HasPublicVersions); err != nil {
			return err
		}
	}
	return nil
}

type dsgVersionMetadata struct {
	Ordinal  *int64 `json:"ordinal"`
	IsPublic bool   `json:"is_public"`
}

func refreshDSGPublicVersions() error {
	token := os.Getenv(DSGTokenVarName)
	if token == "" {
		return fmt.Errorf("%s is unset", DSGTokenVarName)
	}
	base := strings.TrimRight(tc.Auth.DSGAddress, "/") + "/api/dsg/v1"
	serviceQuery := url.QueryEscape(dsgServiceName())
	var datasets []dsgDatasetMetadata
	if err := getDSGJSON(token, base+"/datasets?service="+serviceQuery, &datasets); err != nil {
		return err
	}

	seeds := make([]dvid.VersionID, 0)
	for _, dataset := range datasets {
		if !dataset.HasPublicVersions {
			continue
		}
		root, err := dvid.StringToUUID(dataset.Name)
		if err != nil {
			dvid.Warningf("Skipping DSG public dataset %q: not a DVID root UUID\n", dataset.Name)
			continue
		}
		resolvedRoot, err := datastore.GetRepoRoot(root)
		if err != nil || resolvedRoot != root {
			dvid.Warningf("Skipping DSG public dataset %q: not a local repo root\n", dataset.Name)
			continue
		}
		rootVersion, err := datastore.VersionFromUUID(root)
		if err != nil {
			dvid.Warningf("Skipping DSG public dataset %q: root version is unavailable\n", dataset.Name)
			continue
		}
		var versions []dsgVersionMetadata
		versionsURL := base + "/datasets/" + url.PathEscape(dataset.Name) + "/versions?service=" + serviceQuery
		if err := getDSGJSON(token, versionsURL, &versions); err != nil {
			return err
		}
		for _, version := range versions {
			if !version.IsPublic {
				continue
			}
			if version.Ordinal == nil {
				dvid.Warningf("Skipping public DSG version for %s: null ordinal\n", dataset.Name)
				continue
			}
			if *version.Ordinal <= 0 || *version.Ordinal > int64(^uint32(0)) {
				dvid.Warningf("Skipping public DSG version for %s: ordinal %d is outside DVID range\n", dataset.Name, *version.Ordinal)
				continue
			}
			versionID := dvid.VersionID(*version.Ordinal)
			versionRoot, err := datastore.GetRepoRootVersion(versionID)
			if err != nil {
				dvid.Warningf("Skipping unknown public DSG ordinal %d for %s\n", versionID, dataset.Name)
				continue
			}
			if versionRoot != rootVersion {
				dvid.Warningf("Skipping public DSG ordinal %d for %s: version belongs to another repo\n", versionID, dataset.Name)
				continue
			}
			locked, err := datastore.LockedVersion(versionID)
			if err != nil {
				dvid.Warningf("Skipping public DSG ordinal %d for %s: lock check failed: %v\n", versionID, dataset.Name, err)
				continue
			}
			if !locked {
				dvid.Warningf("Skipping uncommitted public DSG ordinal %d for %s\n", versionID, dataset.Name)
				continue
			}
			seeds = append(seeds, versionID)
		}
	}
	return datastore.SetPublicVersionsFromSeeds(seeds)
}

func getDSGJSON(token, endpoint string, dest interface{}) error {
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return fmt.Errorf("unable to create DSG metadata request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := dsgHTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("unable to contact DSG metadata service: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected DSG metadata response (%d): %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if err := json.NewDecoder(resp.Body).Decode(dest); err != nil {
		return fmt.Errorf("unable to decode DSG metadata response: %v", err)
	}
	return nil
}

func (auth *authData) loadAuthFile() error {
	if len(tc.Auth.AuthFile) == 0 {
		dvid.Infof("No authorization file found.  Proceeding without authorization.\n")
		return nil
	}
	f, err := os.Open(tc.Auth.AuthFile)
	if err != nil {
		return err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	auth.Lock()
	auth.users = make(map[string]string)
	err = json.Unmarshal(data, auth)
	auth.Unlock()
	return err
}

// authConfig holds information on what server to contact for login and other auth settings
type authConfig struct {
	PublicVersions   []string          `toml:"public_versions"`
	ProxyAddress     string            `toml:"proxy_address"`
	AuthFile         string            `toml:"auth_file"`
	Enforce          string            `toml:"enforce"` // "none", "token", "authfile", or "dsg"
	EnforceInternal  string            `toml:"enforce_internal"`
	DSGAddress       string            `toml:"dsg_address"`
	DSGCacheTTL      int               `toml:"dsg_cache_ttl"`
	DSGService       string            `toml:"dsg_service"`
	DSGPublicRefresh int               `toml:"dsg_public_refresh"`
	TrustedProxies   []string          `toml:"trusted_proxies"`
	DatasetMap       map[string]string `toml:"dataset_map"`

	NoEnforce bool `toml:"no_enforce"` // legacy: if true, accept all requests
}

// generateJWT returns a JWT given a user and secret key string.
// This remains for legacy JWT-based auth modes.
func generateJWT(user string) (string, error) {
	if jwtSecretKey == "" {
		return "", fmt.Errorf("Auth token support requires env variable %q to be set", SecretKeyVarName)
	}
	token := jwt.New(jwt.SigningMethodRS512)

	claims := token.Claims.(jwt.MapClaims)
	claims["user"] = user

	tokenString, err := token.SignedString([]byte(jwtSecretKey))
	if err != nil {
		return "", fmt.Errorf("error with JWT signing: %v", err)
	}
	return tokenString, nil
}

func authMode() string {
	enforce := strings.ToLower(tc.Auth.Enforce)
	if tc.Auth.NoEnforce && enforce == "" {
		return "none"
	}
	if enforce != "" {
		return enforce
	}
	if len(tc.Auth.ProxyAddress) != 0 {
		return "token"
	}
	return "none"
}

func validateAuthModes() error {
	if len(tc.Auth.DatasetMap) != 0 {
		return fmt.Errorf("auth.dataset_map is retired: register DVID datasets, aliases and public version anchors in DSG")
	}
	if len(tc.Auth.PublicVersions) != 0 {
		return fmt.Errorf("auth.public_versions is retired: public availability now seeds from DSG metadata")
	}
	if tc.Auth.DSGPublicRefresh < 0 {
		return fmt.Errorf("auth.dsg_public_refresh must be zero or a positive number of seconds")
	}
	if err := validateAuthModeValue("enforce", authMode(), true); err != nil {
		return err
	}
	if tc.Auth.EnforceInternal != "" {
		if err := validateAuthModeValue("enforce_internal", strings.ToLower(tc.Auth.EnforceInternal), false); err != nil {
			return err
		}
	}
	return nil
}

func validateAuthModeValue(name, mode string, allowEmpty bool) error {
	if allowEmpty && mode == "" {
		return nil
	}
	switch mode {
	case "none", "token", "authfile", "dsg":
		return nil
	default:
		return fmt.Errorf("auth.%s has unsupported mode %q", name, mode)
	}
}

func warnDeprecatedAuthMode(mode string) {
	switch mode {
	case "token", "authfile":
		dvid.Warningf("auth mode %q is deprecated; DSG auth is the supported path and legacy JWT modes will be removed in a future release\n", mode)
	}
}

func configureTrustedProxies() error {
	nets := make([]*net.IPNet, 0, len(tc.Auth.TrustedProxies))
	for _, cidr := range tc.Auth.TrustedProxies {
		_, network, err := net.ParseCIDR(cidr)
		if err != nil {
			return fmt.Errorf("bad trusted proxy CIDR %q: %v", cidr, err)
		}
		nets = append(nets, network)
	}
	trustedProxyNets = nets
	return nil
}

func authMiddlewareEnabled() bool {
	return authMode() != "none"
}

func dsgCacheTTL() time.Duration {
	if tc.Auth.DSGCacheTTL <= 0 {
		return 5 * time.Minute
	}
	return time.Duration(tc.Auth.DSGCacheTTL) * time.Second
}

// isPublicRead returns true if the request is a read against a public version.
func isPublicRead(r *http.Request, envUUID interface{}) bool {
	uuid, ok := envUUID.(dvid.UUID)
	if !ok {
		return false
	}
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return datastore.IsPublic(uuid)
	}
	return false
}

func extractDVIDCapability(authHeader string) (token string, present bool) {
	fields := strings.Fields(authHeader)
	if len(fields) == 0 || !strings.EqualFold(fields[0], dvidCapabilityScheme) {
		return "", false
	}
	if len(fields) != 2 || fields[1] == "" {
		return "", true
	}
	return fields[1], true
}

func dvidCapabilityError() error {
	return &authError{status: http.StatusUnauthorized, message: "invalid DVID capability"}
}

func dvidCapabilityConfigurationError(message string) error {
	return &authError{status: http.StatusServiceUnavailable, message: message}
}

func dvidCapabilitySignature(payload []byte, secret string) []byte {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(payload)
	return mac.Sum(nil)
}

func mintDVIDCapability(uuid dvid.UUID, permission, principal string, now time.Time) (string, error) {
	secret := os.Getenv(DVIDCapabilitySecretVarName)
	if secret == "" {
		return "", dvidCapabilityConfigurationError(DVIDCapabilitySecretVarName + " is unset")
	}
	if permission != "view" && permission != "edit" || principal == "" {
		return "", fmt.Errorf("invalid DVID capability subject")
	}
	root, err := datastore.GetRepoRoot(uuid)
	if err != nil {
		return "", fmt.Errorf("could not determine capability root for %s: %v", uuid, err)
	}
	version, err := datastore.VersionFromUUID(uuid)
	if err != nil {
		return "", fmt.Errorf("could not determine capability version for %s: %v", uuid, err)
	}
	nonceBytes := make([]byte, dvidCapabilityNonceBytes)
	if _, err := io.ReadFull(rand.Reader, nonceBytes); err != nil {
		return "", fmt.Errorf("could not create DVID capability nonce: %v", err)
	}
	issuedAt := now.Unix()
	claims := dvidCapabilityClaims{
		FormatVersion: dvidCapabilityFormatVersion,
		Service:       dvidCapabilityService,
		Node:          string(uuid),
		Root:          string(root),
		Version:       uint64(version),
		Permission:    permission,
		Principal:     principal,
		IssuedAt:      issuedAt,
		ExpiresAt:     issuedAt + int64(dvidCapabilityTTL/time.Second),
		Nonce:         base64.RawURLEncoding.EncodeToString(nonceBytes),
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("could not encode DVID capability: %v", err)
	}
	encodedPayload := base64.RawURLEncoding.EncodeToString(payload)
	signature := dvidCapabilitySignature([]byte(encodedPayload), secret)
	return encodedPayload + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

func dvidCapabilityPermissionAllows(granted, required string) bool {
	return granted == "edit" || granted == "view" && required == "view"
}

func verifyDVIDCapability(token string, uuid dvid.UUID, requiredPermission string, now time.Time) (string, error) {
	activeSecret := os.Getenv(DVIDCapabilitySecretVarName)
	if activeSecret == "" {
		return "", dvidCapabilityConfigurationError(DVIDCapabilitySecretVarName + " is unset")
	}
	parts := strings.Split(token, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", dvidCapabilityError()
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", dvidCapabilityError()
	}
	activeMatch := hmac.Equal(signature, dvidCapabilitySignature([]byte(parts[0]), activeSecret))
	previousSecret := os.Getenv(DVIDCapabilityPreviousSecretVarName)
	previousMatch := false
	if previousSecret != "" {
		previousMatch = hmac.Equal(signature, dvidCapabilitySignature([]byte(parts[0]), previousSecret))
	}
	if !activeMatch && !previousMatch {
		return "", dvidCapabilityError()
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", dvidCapabilityError()
	}
	var claims dvidCapabilityClaims
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&claims); err != nil {
		return "", dvidCapabilityError()
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return "", dvidCapabilityError()
	}
	if claims.FormatVersion != dvidCapabilityFormatVersion || claims.Service != dvidCapabilityService ||
		claims.Node != string(uuid) || claims.Principal == "" || claims.Nonce == "" ||
		(claims.Permission != "view" && claims.Permission != "edit") ||
		claims.ExpiresAt-claims.IssuedAt != int64(dvidCapabilityTTL/time.Second) {
		return "", dvidCapabilityError()
	}
	nonce, err := base64.RawURLEncoding.DecodeString(claims.Nonce)
	if err != nil || len(nonce) != dvidCapabilityNonceBytes {
		return "", dvidCapabilityError()
	}
	root, err := datastore.GetRepoRoot(uuid)
	if err != nil || claims.Root != string(root) {
		return "", dvidCapabilityError()
	}
	version, err := datastore.VersionFromUUID(uuid)
	if err != nil || claims.Version != uint64(version) {
		return "", dvidCapabilityError()
	}
	nowUnix := now.Unix()
	skewSeconds := int64(dvidCapabilityClockSkew / time.Second)
	if nowUnix < claims.IssuedAt-skewSeconds || nowUnix > claims.ExpiresAt+skewSeconds {
		return "", dvidCapabilityError()
	}
	if !dvidCapabilityPermissionAllows(claims.Permission, requiredPermission) {
		return "", dvidCapabilityError()
	}
	return claims.Principal, nil
}

func validClioReturnURL(value string) bool {
	if value == "" {
		return true
	}
	parsed, err := url.Parse(value)
	return err == nil && parsed.Host != "" && parsed.User == nil &&
		(parsed.Scheme == "https" || parsed.Scheme == "http")
}

func writeClioAuthResponse(w http.ResponseWriter, response clioAuthResponse) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		dvid.Errorf("unable to encode Clio authorization response: %v\n", err)
	}
}

func clioAuthHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	if authMode() != "dsg" || tc.Auth.DSGAddress == "" {
		writeAuthError(w, r, dvidCapabilityConfigurationError("Clio authorization broker requires top-level DSG enforcement and auth.dsg_address"))
		return
	}
	if os.Getenv(DVIDCapabilitySecretVarName) == "" {
		writeAuthError(w, r, dvidCapabilityConfigurationError(DVIDCapabilitySecretVarName+" is unset"))
		return
	}
	decoder := json.NewDecoder(io.LimitReader(r.Body, 64*1024))
	decoder.DisallowUnknownFields()
	var request clioAuthRequest
	if err := decoder.Decode(&request); err != nil {
		BadRequest(w, r, "malformed Clio authorization request: %v", err)
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		BadRequest(w, r, "Clio authorization request must contain one JSON object")
		return
	}
	if request.Permission != "view" && request.Permission != "edit" {
		BadRequest(w, r, "Clio authorization permission must be view or edit")
		return
	}
	if !validClioReturnURL(request.ReturnURL) {
		BadRequest(w, r, "Clio authorization return_url must be an absolute HTTP(S) URL")
		return
	}
	token := extractBearerToken(r.Header.Get("Authorization"))
	identity, err := getDSGIdentity(token)
	if err != nil {
		writeAuthError(w, r, err)
		return
	}
	principal, err := identity.userName()
	if err != nil {
		writeAuthError(w, r, err)
		return
	}
	uuid, ok := c.Env["uuid"].(dvid.UUID)
	if !ok {
		BadRequest(w, r, "could not determine request UUID for Clio authorization")
		return
	}
	entry, err := dsgEntryForUUIDPermission(uuid, request.Permission)
	if err != nil {
		writeAuthError(w, r, err)
		return
	}
	decision := dsgDecision{Decision: "allow", Roles: []string{"admin"}}
	if !identity.Admin {
		decisions, err := getDSGDecisionsForService(token, dvidCapabilityService, request.ReturnURL, []dsgAuthorizeEntry{entry})
		if err != nil {
			writeAuthError(w, r, err)
			return
		}
		decision = decisions[0]
	}
	response := clioAuthResponse{Decision: decision.Decision, Roles: decision.Roles}
	if decision.Decision == "tos_required" {
		response.TOSURL = decision.TOSURL
	}
	if decision.Decision == "allow" {
		response.Capability, err = mintDVIDCapability(uuid, request.Permission, principal, time.Now())
		if err != nil {
			writeAuthError(w, r, err)
			return
		}
	}
	writeClioAuthResponse(w, response)
}

// isAuthorized authenticates a request and sets c.Env["user"] to the authenticated user.
// Note that the repoRawSelector middleware must be used beforehand so that
// c.Env["uuid"] is properly set for proper handling of public versions.
func isAuthorized(c *web.C, h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		if capability, present := extractDVIDCapability(r.Header.Get("Authorization")); present {
			uuid, ok := c.Env["uuid"].(dvid.UUID)
			if !ok {
				writeAuthError(w, r, dvidCapabilityError())
				return
			}
			requiredPermission := dsgPermissionForRequest(c, r)
			principal, err := verifyDVIDCapability(capability, uuid, requiredPermission, time.Now())
			if err != nil {
				writeAuthError(w, r, err)
				return
			}
			c.Env["user"] = principal
			setAuthenticatedUser(r, principal)
			h.ServeHTTP(w, r)
			return
		}
		if isPublicRead(r, c.Env["uuid"]) {
			h.ServeHTTP(w, r)
			return
		}

		enforce := effectiveEnforce(r)
		if enforce == "none" {
			h.ServeHTTP(w, r)
			return
		}

		if enforce == "dsg" {
			token := extractDSGToken(r)
			identity, err := getDSGIdentity(token)
			if err != nil {
				writeAuthError(w, r, err)
				return
			}
			user, err := identity.userName()
			if err != nil {
				writeAuthError(w, r, err)
				return
			}
			c.Env["user"] = user
			setAuthenticatedUser(r, user)
			if identity.Admin {
				h.ServeHTTP(w, r)
				return
			}
			uuid, ok := c.Env["uuid"].(dvid.UUID)
			if !ok {
				writeAuthError(w, r, fmt.Errorf("could not determine request UUID for DSG auth"))
				return
			}
			entry, err := dsgEntryForUUIDPermission(uuid, dsgPermissionForRequest(c, r))
			if err != nil {
				writeAuthError(w, r, err)
				return
			}
			decisions, err := getDSGDecisions(token, []dsgAuthorizeEntry{entry})
			if err != nil {
				writeAuthError(w, r, err)
				return
			}
			decision := decisions[0]
			switch decision.Decision {
			case "allow":
				h.ServeHTTP(w, r)
				return
			case "tos_required":
				writeAuthError(w, r, &authError{
					status:  http.StatusForbidden,
					message: fmt.Sprintf("terms of service acceptance required for dataset %s: visit %s, then retry", entry.Name, decision.TOSURL),
				})
				return
			default:
				writeAuthError(w, r, &authError{
					status:  http.StatusForbidden,
					message: fmt.Sprintf("user %q does not have sufficient access to DVID repo %q version %s", user, entry.Name, entry.Version),
				})
				return
			}
		}

		reqToken := extractBearerToken(r.Header.Get("Authorization"))
		if reqToken == "" {
			BadRequest(w, r, "JWT required via Authorization in request header")
			return
		}
		token, err := jwt.Parse(reqToken, func(token *jwt.Token) (interface{}, error) {
			return []byte(jwtSecretKey), nil
		})
		if err != nil {
			BadRequest(w, r, "error parsing JWT: %v", err)
			return
		}
		if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
			userClaim, found := claims["user"]
			if found {
				c.Env["user"] = userClaim
			}
			user, ok := userClaim.(string)
			if !ok {
				BadRequest(w, r, "user %v is not a simple string", userClaim)
				return
			}
			if enforce == "authfile" && !userIsAuthorized(user, r.Method) {
				BadRequest(w, r, "user %q is not authorized", user)
				return
			}
		} else {
			BadRequest(w, r, "failed authorization")
			return
		}
		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

func extractBearerToken(authHeader string) string {
	const prefix = "Bearer "
	if len(authHeader) <= len(prefix) || !strings.EqualFold(authHeader[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(authHeader[len(prefix):])
}

func extractDSGToken(r *http.Request) string {
	if token := extractBearerToken(r.Header.Get("Authorization")); token != "" {
		return token
	}
	if cookie, err := r.Cookie("dsg_token"); err == nil && cookie.Value != "" {
		return cookie.Value
	}
	return ""
}

func writeAuthError(w http.ResponseWriter, r *http.Request, err error) {
	var authErr *authError
	if !errors.As(err, &authErr) {
		BadRequest(w, r, err)
		return
	}
	errorMsg := fmt.Sprintf("%s (%s).", authErr.message, r.URL.Path)
	dvid.Errorf("%s\n", errorMsg)
	http.Error(w, errorMsg, authErr.status)
}

type authenticatedUserContextKey struct{}

func setAuthenticatedUser(r *http.Request, user string) {
	*r = *r.WithContext(context.WithValue(r.Context(), authenticatedUserContextKey{}, user))
}

func requestUser(r *http.Request) string {
	if user, ok := r.Context().Value(authenticatedUserContextKey{}).(string); ok && user != "" {
		return user
	}
	return r.URL.Query().Get("u")
}

func getDSGIdentity(token string) (*dsgIdentity, error) {
	if token == "" {
		return nil, &authError{
			status:  http.StatusUnauthorized,
			message: "dsg token required via Authorization header or dsg_token cookie",
		}
	}
	if identity := cachedDSGIdentityForToken(token); identity != nil {
		return identity, nil
	}
	if tc.Auth.DSGAddress == "" {
		return nil, fmt.Errorf("dsg auth requires auth.dsg_address to be configured")
	}

	req, err := http.NewRequest(http.MethodGet, strings.TrimRight(tc.Auth.DSGAddress, "/")+"/api/dsg/v1/user", nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create DSG identity request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := dsgHTTPClient.Do(req)
	if err != nil {
		return nil, &authError{
			status:  http.StatusBadGateway,
			message: fmt.Sprintf("unable to contact DSG auth service: %v", err),
		}
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusUnauthorized, http.StatusForbidden:
		return nil, &authError{
			status:  http.StatusUnauthorized,
			message: "invalid DSG token",
		}
	default:
		body, _ := io.ReadAll(resp.Body)
		return nil, &authError{
			status:  http.StatusBadGateway,
			message: fmt.Sprintf("unexpected DSG auth response (%d): %s", resp.StatusCode, strings.TrimSpace(string(body))),
		}
	}

	var identity dsgIdentity
	if err := json.NewDecoder(resp.Body).Decode(&identity); err != nil {
		return nil, &authError{
			status:  http.StatusBadGateway,
			message: fmt.Sprintf("unable to decode DSG identity response: %v", err),
		}
	}
	if _, err := identity.userName(); err != nil {
		return nil, err
	}
	cacheDSGIdentity(token, &identity)
	return &identity, nil
}

func cachedDSGIdentityForToken(token string) *dsgIdentity {
	ttl := dsgCacheTTL()
	authorizations.RLock()
	entry, found := authorizations.dsgUsers[token]
	authorizations.RUnlock()
	if !found {
		return nil
	}
	if time.Since(entry.fetchedAt) >= ttl {
		authorizations.Lock()
		delete(authorizations.dsgUsers, token)
		authorizations.Unlock()
		return nil
	}
	return entry.identity
}

func cacheDSGIdentity(token string, identity *dsgIdentity) {
	authorizations.Lock()
	if authorizations.dsgUsers == nil {
		authorizations.dsgUsers = make(map[string]cachedDSGIdentity)
	}
	authorizations.dsgUsers[token] = cachedDSGIdentity{identity: identity, fetchedAt: time.Now()}
	authorizations.Unlock()
}

func clearDSGCaches() {
	authorizations.Lock()
	authorizations.dsgUsers = make(map[string]cachedDSGIdentity)
	authorizations.dsgDecisions = make(map[dsgDecisionCacheKey]cachedDSGDecision)
	authorizations.Unlock()
}

func dsgServiceName() string {
	if tc.Auth.DSGService == "" {
		return "dvid"
	}
	return tc.Auth.DSGService
}

func dsgPermissionForMethod(method string) string {
	if requestNeedsOnlyView(method) {
		return "view"
	}
	return "edit"
}

func dsgPermissionForDataRequest(method string, isMutation bool) string {
	if method == http.MethodPost && !isMutation {
		return "view"
	}
	return dsgPermissionForMethod(method)
}

// dsgPermissionForRequest honors datatype declarations for immutable POST
// endpoints, such as neuronjson /query, while failing closed to the normal
// method-based permission for routes that cannot be resolved here.
func dsgPermissionForRequest(c *web.C, r *http.Request) string {
	permission := dsgPermissionForMethod(r.Method)
	if permission == "view" || r.Method != http.MethodPost || c == nil {
		return permission
	}
	uuid, ok := c.Env["uuid"].(dvid.UUID)
	if !ok {
		return permission
	}
	dataname, ok := c.URLParams["dataname"]
	if !ok || dataname == "" {
		return permission
	}
	data, err := datastore.GetDataByUUIDName(uuid, dvid.InstanceName(dataname))
	if err != nil {
		return permission
	}
	return dsgPermissionForDataRequest(
		r.Method, data.IsMutationRequest(r.Method, c.URLParams["keyword"]),
	)
}

func decisionRolesAllow(roles []string, permission string) bool {
	for _, role := range roles {
		switch role {
		case "admin", "manage", "edit":
			return true
		case "view":
			if permission == "view" {
				return true
			}
		}
	}
	return false
}

func requestNeedsOnlyView(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	default:
		return false
	}
}

func dsgDecisionKey(token, service string, entry dsgAuthorizeEntry) dsgDecisionCacheKey {
	return dsgDecisionCacheKey{
		Token: token, Service: service, Name: entry.Name, Branch: entry.Branch,
		Version: entry.Version, Permission: entry.Permission,
	}
}

func cachedDSGDecisionForEntry(token, service string, entry dsgAuthorizeEntry) (dsgDecision, bool) {
	key := dsgDecisionKey(token, service, entry)
	ttl := dsgCacheTTL()
	authorizations.RLock()
	cached, found := authorizations.dsgDecisions[key]
	authorizations.RUnlock()
	if !found {
		return dsgDecision{}, false
	}
	if time.Since(cached.fetchedAt) >= ttl {
		authorizations.Lock()
		delete(authorizations.dsgDecisions, key)
		authorizations.Unlock()
		return dsgDecision{}, false
	}
	return cached.decision, true
}

func cacheDSGDecisions(token, service string, entries []dsgAuthorizeEntry, decisions []dsgDecision) {
	now := time.Now()
	authorizations.Lock()
	if authorizations.dsgDecisions == nil {
		authorizations.dsgDecisions = make(map[dsgDecisionCacheKey]cachedDSGDecision)
	}
	for i, decision := range decisions {
		if decision.Decision == "tos_required" {
			continue
		}
		authorizations.dsgDecisions[dsgDecisionKey(token, service, entries[i])] = cachedDSGDecision{
			decision: decision, fetchedAt: now,
		}
	}
	authorizations.Unlock()
}

func getDSGDecisions(token string, entries []dsgAuthorizeEntry) ([]dsgDecision, error) {
	return getDSGDecisionsForService(token, dsgServiceName(), "", entries)
}

func getDSGDecisionsForService(token, service, returnURL string, entries []dsgAuthorizeEntry) ([]dsgDecision, error) {
	if token == "" {
		return nil, &authError{
			status:  http.StatusUnauthorized,
			message: "dsg token required via Authorization header or dsg_token cookie",
		}
	}
	if len(entries) == 0 {
		return []dsgDecision{}, nil
	}
	if service == "" {
		return nil, fmt.Errorf("dsg authorization service must not be empty")
	}
	if tc.Auth.DSGAddress == "" {
		return nil, fmt.Errorf("dsg auth requires auth.dsg_address to be configured")
	}

	decisions := make([]dsgDecision, len(entries))
	missingEntries := make([]dsgAuthorizeEntry, 0, len(entries))
	missingIndexes := make([]int, 0, len(entries))
	for i, entry := range entries {
		if decision, found := cachedDSGDecisionForEntry(token, service, entry); found {
			decisions[i] = decision
			continue
		}
		missingEntries = append(missingEntries, entry)
		missingIndexes = append(missingIndexes, i)
	}
	if len(missingEntries) == 0 {
		return decisions, nil
	}

	payload, err := json.Marshal(struct {
		Service   string              `json:"service"`
		ReturnURL string              `json:"return_url,omitempty"`
		Entries   []dsgAuthorizeEntry `json:"entries"`
	}{Service: service, ReturnURL: returnURL, Entries: missingEntries})
	if err != nil {
		return nil, fmt.Errorf("unable to encode DSG authorize request: %v", err)
	}
	req, err := http.NewRequest(
		http.MethodPost,
		strings.TrimRight(tc.Auth.DSGAddress, "/")+"/api/dsg/v1/authorize",
		bytes.NewReader(payload),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to create DSG authorize request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := dsgHTTPClient.Do(req)
	if err != nil {
		return nil, &authError{
			status:  http.StatusBadGateway,
			message: fmt.Sprintf("unable to contact DSG auth service: %v", err),
		}
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return nil, &authError{status: http.StatusUnauthorized, message: "invalid DSG token"}
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, &authError{
			status: http.StatusBadGateway,
			message: fmt.Sprintf("unexpected DSG authorize response (%d): %s",
				resp.StatusCode, strings.TrimSpace(string(body))),
		}
	}

	var envelope struct {
		Entries json.RawMessage `json:"entries"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return nil, &authError{
			status:  http.StatusBadGateway,
			message: fmt.Sprintf("unable to decode DSG authorize response: %v", err),
		}
	}
	if len(envelope.Entries) == 0 || bytes.Equal(bytes.TrimSpace(envelope.Entries), []byte("null")) {
		return nil, &authError{status: http.StatusBadGateway, message: "DSG authorize response has no entries list"}
	}
	var responseEntries []dsgDecisionResponse
	if err := json.Unmarshal(envelope.Entries, &responseEntries); err != nil {
		return nil, &authError{
			status:  http.StatusBadGateway,
			message: fmt.Sprintf("DSG authorize response entries are not a list: %v", err),
		}
	}
	if len(responseEntries) != len(missingEntries) {
		return nil, &authError{
			status: http.StatusBadGateway,
			message: fmt.Sprintf("DSG authorize response length mismatch: got %d entries, want %d",
				len(responseEntries), len(missingEntries)),
		}
	}

	fetched := make([]dsgDecision, len(missingEntries))
	for i, responseEntry := range responseEntries {
		requestEntry := missingEntries[i]
		if responseEntry.Name == nil || responseEntry.Branch == nil || responseEntry.Version == nil ||
			*responseEntry.Name != requestEntry.Name || *responseEntry.Branch != requestEntry.Branch ||
			*responseEntry.Version != requestEntry.Version {
			return nil, &authError{
				status:  http.StatusBadGateway,
				message: fmt.Sprintf("DSG authorize response correlation mismatch at entry %d", i),
			}
		}
		decision, err := evaluateDSGDecision(requestEntry, responseEntry)
		if err != nil {
			return nil, err
		}
		fetched[i] = decision
	}
	cacheDSGDecisions(token, service, missingEntries, fetched)
	for i, decision := range fetched {
		decisions[missingIndexes[i]] = decision
	}
	return decisions, nil
}

func evaluateDSGDecision(entry dsgAuthorizeEntry, response dsgDecisionResponse) (dsgDecision, error) {
	decision := dsgDecision{Decision: response.Decision, Roles: response.Roles, TOSURL: response.TOSURL}
	switch response.Decision {
	case "allow":
		if !decisionRolesAllow(response.Roles, entry.Permission) {
			decision.Decision = "deny"
		}
		return decision, nil
	case "deny", "tos_required":
		return decision, nil
	case "service_eval":
		return evaluateDSGAnchors(entry, response.Anchors)
	default:
		return dsgDecision{}, &authError{
			status:  http.StatusBadGateway,
			message: fmt.Sprintf("DSG authorize response has unknown decision %q", response.Decision),
		}
	}
}

func evaluateDSGAnchors(entry dsgAuthorizeEntry, anchors []dsgAnchor) (dsgDecision, error) {
	requestedRaw, err := strconv.ParseUint(entry.Version, 10, 32)
	if err != nil || requestedRaw == 0 {
		return dsgDecision{}, &authError{
			status:  http.StatusBadGateway,
			message: fmt.Sprintf("cannot evaluate DSG anchors for invalid requested version %q", entry.Version),
		}
	}
	requested := dvid.VersionID(requestedRaw)
	requestedRoot, err := datastore.GetRepoRootVersion(requested)
	if err != nil {
		return dsgDecision{}, &authError{
			status:  http.StatusBadGateway,
			message: fmt.Sprintf("cannot resolve requested DVID version %d: %v", requested, err),
		}
	}
	for _, anchor := range anchors {
		if anchor.Version <= 0 || anchor.Version > int64(^uint32(0)) {
			dvid.Warningf("Skipping DSG anchor ordinal %d for %s: outside DVID VersionID range\n", anchor.Version, entry.Name)
			continue
		}
		anchorV := dvid.VersionID(anchor.Version)
		if requested > anchorV {
			continue
		}
		anchorRoot, err := datastore.GetRepoRootVersion(anchorV)
		if err != nil {
			dvid.Warningf("Skipping unknown DSG anchor ordinal %d for %s: %v\n", anchor.Version, entry.Name, err)
			continue
		}
		if anchorRoot != requestedRoot {
			dvid.Warningf("Skipping DSG anchor ordinal %d for %s: version belongs to another repo\n", anchor.Version, entry.Name)
			continue
		}
		precedes, err := datastore.VersionPrecedesOrEquals(requested, anchorV)
		if err != nil {
			return dsgDecision{}, &authError{
				status:  http.StatusBadGateway,
				message: fmt.Sprintf("unable to evaluate DVID ancestry from %d to %d: %v", requested, anchorV, err),
			}
		}
		if precedes && decisionRolesAllow(anchor.Roles, entry.Permission) {
			return dsgDecision{Decision: "allow", Roles: anchor.Roles}, nil
		}
	}
	return dsgDecision{Decision: "deny"}, nil
}

func dsgEntryForUUID(uuid dvid.UUID, method string) (dsgAuthorizeEntry, error) {
	return dsgEntryForUUIDPermission(uuid, dsgPermissionForMethod(method))
}

func dsgEntryForUUIDPermission(uuid dvid.UUID, permission string) (dsgAuthorizeEntry, error) {
	if permission != "view" && permission != "edit" {
		return dsgAuthorizeEntry{}, fmt.Errorf("invalid DSG permission %q", permission)
	}
	root, err := datastore.GetRepoRoot(uuid)
	if err != nil {
		return dsgAuthorizeEntry{}, fmt.Errorf("could not determine root UUID for %s: %v", uuid, err)
	}
	version, err := datastore.VersionFromUUID(uuid)
	if err != nil {
		return dsgAuthorizeEntry{}, fmt.Errorf("could not determine version for %s: %v", uuid, err)
	}
	branch, err := datastore.GetNodeBranch(uuid)
	if err != nil {
		return dsgAuthorizeEntry{}, fmt.Errorf("could not determine branch for %s: %v", uuid, err)
	}
	if branch == "" {
		branch = "master"
	}
	return dsgAuthorizeEntry{
		Name:       string(root),
		Branch:     branch,
		Version:    strconv.FormatUint(uint64(version), 10),
		Permission: permission,
	}, nil
}

func dsgRootProbeEntry(root dvid.UUID) (dsgAuthorizeEntry, error) {
	version, err := datastore.VersionFromUUID(root)
	if err != nil {
		return dsgAuthorizeEntry{}, fmt.Errorf("could not determine root version for %s: %v", root, err)
	}
	return dsgAuthorizeEntry{
		Name:       string(root),
		Branch:     "master",
		Version:    strconv.FormatUint(uint64(version), 10),
		Permission: "view",
	}, nil
}

func effectiveEnforce(r *http.Request) string {
	enforce := authMode()
	if tc.Auth.EnforceInternal == "" {
		return enforce
	}
	if isInternalRequest(r) {
		return strings.ToLower(tc.Auth.EnforceInternal)
	}
	return enforce
}

func isInternalRequest(r *http.Request) bool {
	if !strings.EqualFold(r.Header.Get(internalHeader), "true") {
		return false
	}
	if len(trustedProxyNets) == 0 {
		return true
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	for _, network := range trustedProxyNets {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

// adminPrivileged is the single admin predicate for all server gates: a
// request has admin privileges iff it carries a valid admintoken or comes
// from an authenticated DSG admin user.  It derives everything from the
// request itself so it can be evaluated by middleware that runs before
// authentication (adminPrivHandler precedes repoRawSelector and isAuthorized)
// and by datatype handlers that never see the middleware env.  It is
// deliberately not requestAccessScope().full, which is also true for every
// request when enforce=none.
func adminPrivileged(r *http.Request) bool {
	return adminTokenPrivileged(r) || dsgAdminPrivileged(r)
}

// adminTokenPrivileged returns true if the request carries the configured
// admintoken.  Every grant is audit-logged with client info since the token
// carries no identity.
func adminTokenPrivileged(r *http.Request) bool {
	if len(adminToken) == 0 || r.URL.Query().Get("admintoken") != adminToken {
		return false
	}
	dvid.Infof("admin privilege granted via admintoken to %s for %s %s\n", r.RemoteAddr, r.Method, r.URL.Path)
	return true
}

// dsgAdminPrivileged returns true if the request carries a token that
// validates to a DSG admin user.  Only consulted when a DSG auth mode is
// configured so legacy-JWT bearer tokens never trigger DSG service lookups.
func dsgAdminPrivileged(r *http.Request) bool {
	if authMode() != "dsg" && strings.ToLower(tc.Auth.EnforceInternal) != "dsg" {
		return false
	}
	token := extractDSGToken(r)
	if token == "" {
		return false
	}
	identity, err := getDSGIdentity(token)
	if err != nil {
		return false
	}
	return identity.Admin
}

// AdminPrivileged returns true if the request has admin privileges: a valid
// admintoken or an authenticated DSG admin user.  Datatype handlers should
// use this to gate admin-only endpoints since they don't have access to the
// server middleware environment.
func AdminPrivileged(r *http.Request) bool {
	return adminPrivileged(r)
}

type accessScope struct {
	internal  bool
	adminPriv bool
	identity  *dsgIdentity
	token     string
	full      bool
}

func requestAccessScope(c web.C, r *http.Request) accessScope {
	scope := accessScope{internal: isInternalRequest(r)}
	if adminPriv, ok := c.Env["adminPriv"].(bool); ok {
		scope.adminPriv = adminPriv
	}

	enforce := effectiveEnforce(r)
	if enforce == "none" || scope.adminPriv {
		scope.full = true
		return scope
	}

	switch enforce {
	case "dsg":
		token := extractDSGToken(r)
		identity, err := getDSGIdentity(token)
		if err != nil {
			return scope
		}
		scope.identity = identity
		scope.token = token
		scope.full = identity.Admin
	case "token", "authfile":
		scope.full = legacyJWTAuthorized(r, enforce)
	}
	return scope
}

func (scope accessScope) repoVisibility(rootUUID dvid.UUID, method string) (datastore.RepoVisibility, error) {
	if scope.full {
		return datastore.RepoFullView, nil
	}
	if scope.identity != nil {
		entry, err := dsgRootProbeEntry(rootUUID)
		if err != nil {
			return datastore.RepoHidden, err
		}
		decisions, err := getDSGDecisions(scope.token, []dsgAuthorizeEntry{entry})
		if err != nil {
			return datastore.RepoHidden, err
		}
		if decisions[0].Decision == "allow" {
			return datastore.RepoFullView, nil
		}
	}
	return datastore.RepoPublicOnly, nil
}

func (scope accessScope) repoVisibilityForUUID(uuid dvid.UUID, method string) (datastore.RepoVisibility, error) {
	rootUUID, err := datastore.GetRepoRoot(uuid)
	if err != nil {
		return datastore.RepoHidden, err
	}
	return scope.repoVisibility(rootUUID, method)
}

func (scope accessScope) repoVisibilities(roots map[dvid.UUID]dvid.VersionID) (map[dvid.UUID]datastore.RepoVisibility, error) {
	visibilities := make(map[dvid.UUID]datastore.RepoVisibility, len(roots))
	if scope.full {
		for root := range roots {
			visibilities[root] = datastore.RepoFullView
		}
		return visibilities, nil
	}
	if scope.identity == nil {
		for root := range roots {
			visibilities[root] = datastore.RepoPublicOnly
		}
		return visibilities, nil
	}
	entries := make([]dsgAuthorizeEntry, 0, len(roots))
	orderedRoots := make([]dvid.UUID, 0, len(roots))
	for root, version := range roots {
		orderedRoots = append(orderedRoots, root)
		entries = append(entries, dsgAuthorizeEntry{
			Name:       string(root),
			Branch:     "master",
			Version:    strconv.FormatUint(uint64(version), 10),
			Permission: "view",
		})
	}
	decisions, err := getDSGDecisions(scope.token, entries)
	if err != nil {
		return nil, err
	}
	for i, root := range orderedRoots {
		if decisions[i].Decision == "allow" {
			visibilities[root] = datastore.RepoFullView
		} else {
			visibilities[root] = datastore.RepoPublicOnly
		}
	}
	return visibilities, nil
}

func legacyJWTAuthorized(r *http.Request, enforce string) bool {
	reqToken := extractBearerToken(r.Header.Get("Authorization"))
	if reqToken == "" {
		return false
	}
	token, err := jwt.Parse(reqToken, func(token *jwt.Token) (interface{}, error) {
		return []byte(jwtSecretKey), nil
	})
	if err != nil {
		return false
	}
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok || !token.Valid {
		return false
	}
	userClaim, found := claims["user"]
	if !found {
		return false
	}
	user, ok := userClaim.(string)
	if !ok {
		return false
	}
	return enforce != "authfile" || userIsAuthorized(user, r.Method)
}

// userIsAuthorized returns true if the user is in our authorization file
func userIsAuthorized(user string, httpMethod string) bool {
	if len(authorizations.users) == 0 {
		return false
	}
	authorizations.RLock()
	priv, found := authorizations.users[user]
	authorizations.RUnlock()
	if !found {
		authorizations.RLock()
		priv, found = authorizations.users["*"]
		authorizations.RUnlock()
		if !found {
			return false
		}
	}
	var readReq bool
	switch httpMethod {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		readReq = true
	}
	switch priv {
	case "readwrite":
		return true
	case "read":
		return readReq
	case "write":
		return !readReq
	default:
		dvid.Errorf("Authorized user %q has unparsable privilege %q\n", user, priv)
		return false
	}
}

// contacts proxy server and returns email
func getEmailFromProxy(r *http.Request) (string, error) {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{
		Timeout:   time.Second * 30,
		Transport: tr,
	}
	profileURL := "https://" + strings.TrimSuffix(tc.Auth.ProxyAddress, "/") + "/profile"
	req, err := http.NewRequest(http.MethodGet, profileURL, nil)
	if err != nil {
		return "", fmt.Errorf("unable to create new /profile request: %v", err)
	}

	for _, cookie := range r.Cookies() {
		req.AddCookie(cookie)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("unable to get profile from %s: %v", tc.Auth.ProxyAddress, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unable to get profile from %s (status %d), perhaps not logged in: %v", tc.Auth.ProxyAddress, resp.StatusCode, err)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("unable to read /profile response from %s: %v", tc.Auth.ProxyAddress, err)
	}
	var profileData map[string]string
	if err := json.Unmarshal(data, &profileData); err != nil {
		return "", fmt.Errorf("unable to decode JSON for profile: %v", err)
	}
	user := profileData["Email"]
	if len(user) == 0 {
		return "", fmt.Errorf("unable to get user (email) from proxy %s", tc.Auth.ProxyAddress)
	}
	return user, nil
}

// handler for /api/server/token requests
func serverTokenHandler(w http.ResponseWriter, r *http.Request) {
	if authMode() == "dsg" {
		BadRequest(w, r, "DSG auth mode uses DSG tokens directly; /api/server/token is not used")
		return
	}

	var email string
	if len(tc.Auth.ProxyAddress) != 0 { // do legacy proxy auth server
		var err error
		email, err = getEmailFromProxy(r)
		if err != nil {
			BadRequest(w, r, "unable to get token from proxy: %v", err)
			return
		}
	} else { // use Google ID authentication
		authToken := r.Header.Get("Authorization")
		oauth2Service, err := oauth2.New(httpClient)
		tokenInfoCall := oauth2Service.Tokeninfo()
		tokenInfoCall.IdToken(authToken)
		tokenInfo, err := tokenInfoCall.Do()
		if err != nil {
			BadRequest(w, r, "unable to verify auth header: %v", err)
			return
		}
		email = tokenInfo.Email
	}

	// generate JWT
	tokenString, err := generateJWT(email)
	if err != nil {
		BadRequest(w, r, "unable to generate JWT: %v", err)
		return
	}
	dvid.Infof("Returning JWT for user %s.\n", email)
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprint(w, tokenString)
}
