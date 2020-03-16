package server

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	jwt "github.com/dgrijalva/jwt-go"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/zenazn/goji/web"
)

// global authorization list.
// TODO: more elaborate version- and instance-based ACL.
var authorizedUsers map[string]string

// authConfig holds information on what server to contact for login and the
type authConfig struct {
	ProxyAddress string `toml:"proxy_address"`
	AuthFile     string `toml:"auth_file"`
	SecretKey    string `toml:"secret_key"`
}

// generateJWT returns a JWT given a user and secret key string
func generateJWT(user string) (string, error) {
	token := jwt.New(jwt.SigningMethodHS256)

	claims := token.Claims.(jwt.MapClaims)
	claims["user"] = user

	tokenString, err := token.SignedString([]byte(tc.Auth.SecretKey))
	if err != nil {
		return "", fmt.Errorf("error with JWT signing: %v", err)
	}
	return tokenString, nil
}

// isAuthorized is middleware that validates a JWT and sets the c.Env["user"] field
// to the authenticated user.
func isAuthorized(c *web.C, h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		reqToken := r.Header.Get("Authorization")
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
				if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
					return nil, fmt.Errorf("error signing method: %v", token.Header["alg"])
				}
				return []byte(tc.Auth.SecretKey), nil
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
				if !globalIsAuthorized(user, r.Method) {
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

func loadAuthFile() error {
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
	if err := json.Unmarshal(data, &authorizedUsers); err != nil {
		return err
	}
	return nil
}

// globalIsAuthorized returns true if the user is in our authorization file
func globalIsAuthorized(user string, httpMethod string) bool {
	if len(authorizedUsers) == 0 {
		return false
	}
	method := strings.ToLower(httpMethod)
	readReq := method == "get" || method == "head"
	priv, found := authorizedUsers[user]
	if !found {
		priv, found = authorizedUsers["*"]
		if !found {
			return false
		}
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
