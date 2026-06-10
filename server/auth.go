package server

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
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

const internalHeader = "X-DVID-Internal"

func init() {
	jwtSecretKey = os.Getenv(SecretKeyVarName)
}

// authData holds legacy auth-file users and the DSG user cache.
type authData struct {
	sync.RWMutex
	users    map[string]string
	dsgUsers map[string]cachedDSGUser
}

type cachedDSGUser struct {
	user      *dsgUserCache
	fetchedAt time.Time
}

type dsgUserCache struct {
	Email         string              `json:"email"`
	Name          string              `json:"name"`
	Admin         bool                `json:"admin"`
	Groups        []string            `json:"groups"`
	PermissionsV2 map[string][]string `json:"permissions_v2"`
	DatasetsAdmin []string            `json:"datasets_admin"`
}

type authError struct {
	status  int
	message string
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
	auth.dsgUsers = make(map[string]cachedDSGUser)
	auth.Unlock()
	return nil
}

// PublishPublicVersions validates and publishes the configured public release set.
// It must be called after datastore.Initialize(), when the repo manager is loaded.
func PublishPublicVersions() error {
	if err := datastore.SetPublicVersions(tc.Auth.PublicVersions); err != nil {
		dvid.Errorf("unable to set public UUIDs due to error: %v\n", err)
		return err
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
	PublicVersions  []string          `toml:"public_versions"`
	ProxyAddress    string            `toml:"proxy_address"`
	AuthFile        string            `toml:"auth_file"`
	Enforce         string            `toml:"enforce"` // "none", "token", "authfile", or "dsg"
	EnforceInternal string            `toml:"enforce_internal"`
	DSGAddress      string            `toml:"dsg_address"`
	DSGCacheTTL     int               `toml:"dsg_cache_ttl"`
	TrustedProxies  []string          `toml:"trusted_proxies"`
	DatasetMap      map[string]string `toml:"dataset_map"`

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

// isAuthorized authenticates a request and sets c.Env["user"] to the authenticated user.
// Note that the repoRawSelector middleware must be used beforehand so that
// c.Env["uuid"] is properly set for proper handling of public versions.
func isAuthorized(c *web.C, h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
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
			user, err := getDSGUser(extractDSGToken(r))
			if err != nil {
				writeAuthError(w, r, err)
				return
			}
			c.Env["user"] = user.Email
			if user.Admin {
				h.ServeHTTP(w, r)
				return
			}
			datasetID, err := dsgDatasetForRequest(c.Env["uuid"])
			if err != nil {
				writeAuthError(w, r, err)
				return
			}
			if !userHasDatasetAccess(user, datasetID, r.Method) {
				writeAuthError(w, r, &authError{
					status:  http.StatusForbidden,
					message: fmt.Sprintf("user %q does not have sufficient access to dataset %q", user.Email, datasetID),
				})
				return
			}
			h.ServeHTTP(w, r)
			return
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

func getDSGUser(token string) (*dsgUserCache, error) {
	if token == "" {
		return nil, &authError{
			status:  http.StatusUnauthorized,
			message: "dsg token required via Authorization header or dsg_token cookie",
		}
	}
	if user := cachedDSGUserForToken(token); user != nil {
		return user, nil
	}
	if tc.Auth.DSGAddress == "" {
		return nil, fmt.Errorf("dsg auth requires auth.dsg_address to be configured")
	}

	req, err := http.NewRequest(http.MethodGet, strings.TrimRight(tc.Auth.DSGAddress, "/")+"/api/v1/user/cache", nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create DSG user cache request: %v", err)
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

	var user dsgUserCache
	if err := json.NewDecoder(resp.Body).Decode(&user); err != nil {
		return nil, fmt.Errorf("unable to decode DSG user cache response: %v", err)
	}
	cacheDSGUser(token, &user)
	return &user, nil
}

func cachedDSGUserForToken(token string) *dsgUserCache {
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
	return entry.user
}

func cacheDSGUser(token string, user *dsgUserCache) {
	authorizations.Lock()
	if authorizations.dsgUsers == nil {
		authorizations.dsgUsers = make(map[string]cachedDSGUser)
	}
	authorizations.dsgUsers[token] = cachedDSGUser{user: user, fetchedAt: time.Now()}
	authorizations.Unlock()
}

func clearDSGUserCache() {
	authorizations.Lock()
	authorizations.dsgUsers = make(map[string]cachedDSGUser)
	authorizations.Unlock()
}

func dsgDatasetForRequest(envUUID interface{}) (string, error) {
	uuid, ok := envUUID.(dvid.UUID)
	if !ok {
		return "", fmt.Errorf("could not determine request UUID for DSG auth")
	}
	rootUUID, err := datastore.GetRepoRoot(uuid)
	if err != nil {
		return "", fmt.Errorf("could not determine root UUID for %s: %v", uuid, err)
	}
	return dsgDatasetForRootUUID(rootUUID)
}

func dsgDatasetForRootUUID(rootUUID dvid.UUID) (string, error) {
	if len(tc.Auth.DatasetMap) == 0 {
		return "", &authError{
			status:  http.StatusForbidden,
			message: "dsg auth requires auth.dataset_map to be configured",
		}
	}
	datasetID, found := tc.Auth.DatasetMap[string(rootUUID)]
	if !found || datasetID == "" {
		return "", &authError{
			status:  http.StatusForbidden,
			message: fmt.Sprintf("no DSG dataset mapping configured for root UUID %s", rootUUID),
		}
	}
	return datasetID, nil
}

func requestNeedsOnlyView(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	default:
		return false
	}
}

func userHasDatasetAccess(user *dsgUserCache, datasetID, method string) bool {
	if user == nil {
		return false
	}
	for _, adminDataset := range user.DatasetsAdmin {
		if adminDataset == datasetID {
			return true
		}
	}
	perms, found := user.PermissionsV2[datasetID]
	if !found {
		return false
	}
	readOnly := requestNeedsOnlyView(method)
	for _, perm := range perms {
		switch perm {
		case "admin", "manage", "edit":
			return true
		case "view":
			if readOnly {
				return true
			}
		}
	}
	return false
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
	user, err := getDSGUser(token)
	if err != nil {
		return false
	}
	return user.Admin
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
	user      *dsgUserCache
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
		user, err := getDSGUser(extractDSGToken(r))
		if err != nil {
			return scope
		}
		scope.user = user
		scope.full = user.Admin
	case "token", "authfile":
		scope.full = legacyJWTAuthorized(r, enforce)
	}
	return scope
}

func (scope accessScope) repoVisibility(rootUUID dvid.UUID, method string) datastore.RepoVisibility {
	if scope.full {
		return datastore.RepoFullView
	}
	if scope.user != nil {
		datasetID, err := dsgDatasetForRootUUID(rootUUID)
		if err == nil && userHasDatasetAccess(scope.user, datasetID, method) {
			return datastore.RepoFullView
		}
	}
	return datastore.RepoPublicOnly
}

func (scope accessScope) repoVisibilityForUUID(uuid dvid.UUID, method string) (datastore.RepoVisibility, error) {
	rootUUID, err := datastore.GetRepoRoot(uuid)
	if err != nil {
		return datastore.RepoHidden, err
	}
	return scope.repoVisibility(rootUUID, method), nil
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
