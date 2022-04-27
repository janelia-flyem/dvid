package server

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
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
	authorizations authData
	jwtSecretKey   string
	httpClient     = &http.Client{}
)

// SecretKeyVarName is the environment variable holding the secret key for JWT support.
const SecretKeyVarName = "DVID_JWT_SECRET_KEY"

func init() {
	jwtSecretKey = os.Getenv(SecretKeyVarName)
}

// authorization data handling both public versions and user-specific permissions.
type authData struct {
	sync.RWMutex
	users  map[string]string
	public dvid.UUIDSet
}

func (auth *authData) initialize() error {
	if err := auth.loadAuthFile(); err != nil {
		return err
	}
	auth.Lock()
	auth.public = make(dvid.UUIDSet)
	for _, uuidStr := range tc.Auth.PublicVersions {
		uuid, _, err := datastore.MatchingUUID(uuidStr)
		if err != nil {
			dvid.Errorf("unable to set public UUIDs due to error: %v\n", err)
			auth.Unlock()
			return err
		}
		auth.public[uuid] = struct{}{}
	}
	auth.Unlock()
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
	data, err := ioutil.ReadAll(f)
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
	PublicVersions []string `toml:"public_versions"`
	ProxyAddress   string   `toml:"proxy_address"`
	AuthFile       string   `toml:"auth_file"`
	Enforce        string   `toml:"enforce"` // either "none", "token" or "authfile"

	NoEnforce bool `toml:"no_enforce"` // legacy: if true, accept all requests
}

// generateJWT returns a JWT given a user and secret key string
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

// isPublic returns true if the request is a read and the version is
// listed as a public version.
func isPublic(r *http.Request, envUUID interface{}) bool {
	uuid, ok := envUUID.(dvid.UUID)
	if !ok {
		return false
	}
	authorizations.RLock()
	var canRead bool
	if _, isPublic := authorizations.public[uuid]; isPublic {
		switch r.Method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
			canRead = true
		}
	}
	authorizations.RUnlock()
	return canRead
}

// isAuthorized is middleware that validates a JWT and sets the c.Env["user"] field
// to the authenticated user.
// Note that the repoRawSelector middleware must be used beforehand so that
// c.Env["uuid"] is properly set for proper handling of public versions.
func isAuthorized(c *web.C, h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		if isPublic(r, c.Env["uuid"]) {
			h.ServeHTTP(w, r)
			return
		}
		reqToken := r.Header.Get("Authorization")
		enforce := strings.ToLower(tc.Auth.Enforce)
		if tc.Auth.NoEnforce && enforce == "" {
			enforce = "none"
		}
		if enforce == "none" {
			h.ServeHTTP(w, r)
			return
		}
		if len(reqToken) == 0 {
			BadRequest(w, r, "JWT required via Authorization in request header")
			return
		}
		splitToken := strings.Split(reqToken, "Bearer")
		if len(splitToken) != 2 {
			BadRequest(w, r, "bearer not in proper format")
			return
		}
		reqToken = strings.TrimSpace(splitToken[1])
		if len(reqToken) != 0 {
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
					BadRequest(w, r, "user %v is not a simple string", user)
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
		} else {
			BadRequest(w, r, "requests require JWT authentication")
			return
		}
		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
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
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unable to get profile from %s (status %d), perhaps not logged in: %v", tc.Auth.ProxyAddress, resp.StatusCode, err)
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
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
	fmt.Fprintf(w, tokenString)
}
