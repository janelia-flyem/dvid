/*
	Package service implements DVID support for associating external programs
        as a service exposed in the DVID API.
*/
package service

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/keyvalue"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	Version = "0.1"
	RepoUrl = "github.com/janelia-flyem/dvid/service"
)

const HelpMessage = `
API for 'service' datatype (github.com/janelia-flyem/dvid/service)
=============================================================================

Command-line
 
$ dvid dataset <UUID> new <service type name> <service name> <settings>

	Adds newly named service of the 'service type name' to dataset with specified UUID.
        TBD: In the future, each service type instantiation should be instantiated once
        or for each dataset when the DVID server starts.  The name should also be the same
        as the service type name.  Also, there could be settings that need to be recomputed
        whenever DVID is started, such as the location of the service.  If this is the case,
        the instantiated service should be temporary, but the data that it points too
        (i.e., the specific invocations of the particular service should be stored in the DB.

	Example:

	$ dvid dataset 3f8c new boundpred boundpred

    Arguments:

    UUID                Hexidecimal string with enough characters to uniquely identify a version node.
    service type name   Data type name, e.g., "boundpred"
    service name        Name of service to create (should be same as the type name)
    settings            TBD: perhaps the settings for how to connect to the service 

(following command-line commands not yet implemented and maybe unnecessary)

$ dvid node <UUID> <service name> get <service id>
$ dvid node <UUID> <service name> get result <service id>

    Returns the status or results of a service.  The service results can be empty or be
    any binary type as specified by the particular service.  When a service is executed
    a callback service id is returned that can be used to look up the status and results.
    
    Example: 

    $ dvid node 3f8c boundpred get 3 > status
    $ dvid node 3f8c boundpred get result 3 > data.json


    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    service name  Name of the service.
    service id    ID of the specific invocation of the service

        
$ dvid node <UUID> <service name> post < <json name>

    Posts parameter information to the service which instantiates a service call and returns
    a callback location.

    Example: 

    $ dvid node 3f8c boundpred post < parameters.json

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    service name     Name of the service that will be invoked.
    json name     Json data to be stored (you must pipe the json data in).


$ dvid node <UUID> <service name> put <service id> < <status>
$ dvid node <UUID> <service name> put result <service id> < <results>

    Put a status message or results at a particular service id.  Only the invoked service
    should have the private key required to authenticate this operation.  In other
    words, this should never be called directly by the user. 

        
    ------------------

HTTP API (Level 2 REST):

Note that browsers support HTTP PUT and DELETE via javascript but only GET/POST are
included in HTML specs.  For ease of use in constructing clients, HTTP POST is used
to create or modify resources in an idempotent fashion.

GET  /api/node/<UUID>/<service name>/help
GET  /api/node/<UUID>/<service name>/contract

	Returns service-specific help or interface message.


GET  /api/node/<UUID>/<service name>/<service id>[/result]
PUT  /api/node/<UUID>/<service name>/<service id>[/result]

    Retrieves or puts values for a given service call.
    PUT data should use the "status" key in a JSON when updating the status and
    "data" key in a form when updating the result (/result specified).

    Example: 

    GET /api/node/3f8c/boundpred/5

    Returns the status, in plain text, associated with service invocation '5'.  If
    a result is specified, it returns a binary file representing the result (see
    documentation for specific service name).

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    service name  Name of service to add/retrieve.
    service id    Name of service invocation to add/retrieve.

POST  /api/node/<UUID>/<service name>
    
    Creates a service invocation using the JSON data specified.

    Example:
    
    POST /api/node/3f8c/boundpred
    
    Returns the resource ID for the new service invocation ("service id" in json).

    Arguments:

    UUID          Hexidecimal string with enough characters to uniquely identify a version node.
    service name  Name of service to be invoked.
`

func init() {
	// Need to register types that will be used to fulfill interfaces.
	gob.Register(&Datatype{})
	gob.Register(&Data{})
	gob.Register(&binary.LittleEndian)
	gob.Register(&binary.BigEndian)
	gob.Register(&ServiceLocalExe{})

	rand.Seed(time.Now().UTC().UnixNano())
}

func randomHex() (randomStr string) {
	randomStr = ""
	for i := 0; i < 16; i++ {
		val := rand.Intn(16)
		randomStr += strconv.FormatInt(int64(val), 16)
	}
	return
}

// interface for actually calling the service
type ServiceExecutor interface {
	RunService(jsonStr string)
}

// runs service based on local executable, implements ServiceExecutor interface
type ServiceLocalExe struct {
	// name or path to call service
	Exeloc string
}

// NewServiceExe returns a pointer to ServiceExe
func NewServiceLocalExe(exeloc string) (serviceExe *ServiceLocalExe) {
	serviceExe = new(ServiceLocalExe)
	serviceExe.Exeloc = exeloc
	return
}

// call the service with the json input provided by the user
func (s *ServiceLocalExe) RunService(jsonStr string) {
	cmd := exec.Command(s.Exeloc)
	in, _ := cmd.StdinPipe()

	// pipe the json string as standard input to the program
	//in.Write([]byte(jsonStr))
	fmt.Fprintln(in, jsonStr)

	// do not wait for command completion
	cmd.Start()
	in.Close()
}

// Datatype embeds the datastore's Datatype to create a unique type for keyvalue functions.
type Datatype struct {
	datastore.Datatype
	ServiceExe ServiceExecutor
	Contract   string
}

// NewDatatype returns a pointer to a new keyvalue Datatype with default values set.
func NewDatatype(serviceExe ServiceExecutor, contract string) (dtype *Datatype) {
	dtype = new(Datatype)
	dtype.ServiceExe = serviceExe
	dtype.Contract = contract
	dtype.Requirements = &storage.Requirements{
		BulkIniter: false,
		BulkWriter: false,
		Batcher:    true,
	}
	return
}

// --- TypeService interface ---

// NewData returns a pointer to new keyvalue data with default values.
func (dtype *Datatype) NewDataService(id *datastore.DataID, c dvid.Config) (datastore.DataService, error) {
	basedata, err := datastore.NewDataService(id, dtype, c)
	if err != nil {
		return nil, err
	}
	return &Data{
		Data:             &keyvalue.Data{basedata},
		ServiceExe:       dtype.ServiceExe,
		Contract:         dtype.Contract,
		CurrentServiceId: 0,
	}, nil
}

func (dtype *Datatype) Help() string {
	return fmt.Sprintf(HelpMessage)
}

// Data embeds the datastore's Data and extends it with keyvalue properties (none for now).
type Data struct {
	*keyvalue.Data
	ServiceExe       ServiceExecutor
	Contract         string
	CurrentServiceId int32
}

// JSONContractString returns the JSON for this Service's contract
func (d *Data) ContractString() (jsonStr string, err error) {
	return fmt.Sprintf(d.Contract), err
}

// --- DataService interface ---

// DoRPC acts as a switchboard for RPC commands.
func (d *Data) DoRPC(request datastore.Request, reply *datastore.Response) error {
	// TBD -- maybe
	return fmt.Errorf("RPC commands not yet implemented")
}

func (d *Data) PostService(uuid datastore.UUID, w http.ResponseWriter, r *http.Request) error {
	// retrieves contract from http
	decoder := json.NewDecoder(r.Body)
	contract := make(map[string]interface{})
	err := decoder.Decode(&contract)
	if err != nil {
		return fmt.Errorf("Error decoding POSTed JSON contract for calling service")
	}

	// location for writing back status and results
	callback := r.URL.Path + "/" + strconv.Itoa(int(d.CurrentServiceId))

	// parameters the service will need to fulfill the contract

	// the service understand how to call dvid given a UUID and base URL
	contract["server-path"] = server.ServerAddress()
	contract["uuid"] = uuid

	contract["callback"] = callback
	contract["status"] = "not started"

	// random string for future communication be service and DVID
	contract["access-key"] = randomHex()

	contractJSON, err := json.Marshal(contract)
	if err != nil {
		return fmt.Errorf("JSON string could not be formatted properly to call service")
	}

	// add contract to a value associated with the service id
	err = d.PutData(uuid, strconv.Itoa(int(d.CurrentServiceId)), contractJSON)
	if err != nil {
		return fmt.Errorf("Service call information could not be saved in the DB")
	}

	// increase the unique id associated with each service call
	d.CurrentServiceId += 1

	// save the state of the service since it has changed
	service := server.DatastoreService()
	err = service.SaveDataset(uuid)
	if err != nil {
		return fmt.Errorf("Error in trying to save dataset on change: %s", err.Error())
	}

	// convert the bytes to string and execute the service
	jstr := string(contractJSON)
	go d.ServiceExe.RunService(jstr)

	// create json just for the callback address to be returned to the caller
	callbackJSON, err := json.Marshal(map[string]string{
		"callback": callback,
	})
	if err != nil {
		return fmt.Errorf("JSON could not be created from callback")
	}

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, string(callbackJSON))

	return nil
}

func (d *Data) BasicAuthKey(r *http.Request) (accessKey string, err error) {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		err = fmt.Errorf("No authentication with access key")
		return
	}

	authSlice := strings.Split(strings.TrimSpace(auth), "Basic ")
	if len(authSlice) != 2 {
		err = fmt.Errorf("Incorrectly formatted authentication")
		return
	}

	authBytes, err := base64.StdEncoding.DecodeString(authSlice[1])
	if err != nil {
		return
	}

	s := strings.Split(string(authBytes), ":")
	accessKey = s[0]

	return
}

func (d *Data) AuthenticateRequest(accessKey string, r *http.Request) error {
	requestKey, err := d.BasicAuthKey(r)
	if err != nil {
		return err
	}
	if requestKey != accessKey {
		return fmt.Errorf("Wrong")
	}
	return nil
}

// DoHTTP handles all incoming HTTP requests for this data.
func (d *Data) DoHTTP(uuid datastore.UUID, w http.ResponseWriter, r *http.Request) error {
	// Allow cross-origin resource sharing.
	w.Header().Add("Access-Control-Allow-Origin", "*")

	// Break URL request into arguments
	url := r.URL.Path[len(server.WebAPIPath):]
	parts := strings.Split(url, "/")
	method := strings.ToLower(r.Method)

	if len(parts) == 3 {
		// service invocation
		if method == "post" {
			err := d.PostService(uuid, w, r)
			if err != nil {
				return err
			}
		} else {
			return fmt.Errorf("Only posts allowed on a service name")
		}
	} else if len(parts) > 3 {
		// Process help and info.
		if method == "get" {
			switch parts[3] {
			case "help":
				w.Header().Set("Content-Type", "text/plain")
				fmt.Fprintln(w, d.Help())
				return nil
			case "contract":
				w.Header().Set("Content-Type", "text/plain")
				fmt.Fprintln(w, d.Contract)
				return nil
			default:
			}
		}

		// get or put on specific service call id key
		serviceIdStr := parts[3]
		jsonBytes, err := d.GetData(uuid, serviceIdStr)
		if err != nil {
			return err
		}

		serviceData := make(map[string]interface{})
		err = json.Unmarshal(jsonBytes, &serviceData)
		if err != nil {
			return err
		}

		accessKey, ok := serviceData["access-key"]
		accessKeyStr := accessKey.(string)
		if !ok {
			return fmt.Errorf("Service ID %s does not have an access key", serviceIdStr)
		}

		if len(parts) == 4 {
			// querying or putting the status of the service ID
			if method == "get" {
				status, ok := serviceData["status"]
				if !ok {
					return fmt.Errorf("No status set for %s/%s", parts[2], serviceIdStr)
				}
				jsonBytes, err := json.Marshal(map[string]string{"status": status.(string)})

				if err != nil {
					return fmt.Errorf("Could not create a json string")
				}

				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintf(w, string(jsonBytes))

			} else if method == "put" {
				// must be authenticated
				err = d.AuthenticateRequest(accessKeyStr, r)
				if err != nil {
					return err
				}

				// grab "status" from json in request
				decoder := json.NewDecoder(r.Body)
				statusData := make(map[string]interface{})
				err = decoder.Decode(&statusData)
				if err != nil {
					return fmt.Errorf("Error decoding PUT JSON status for %s/%s", parts[2], parts[3])
				}
				status, ok := statusData["status"]
				if !ok {
					return fmt.Errorf("No status set in status field")
				}

				// replace service data's status with supplied status
				serviceData["status"] = status
				jsonData, err := json.Marshal(serviceData)
				if err != nil {
					return fmt.Errorf("JSON could not be formatted properly")
				}

				// put this back in the key/value database as json
				err = d.PutData(uuid, serviceIdStr, jsonData)
				if err != nil {
					return fmt.Errorf("Service call information could not be saved in the DB")
				}
			} else {
				return fmt.Errorf("The service ID only supports GET and PUT")
			}
		} else if len(parts) == 5 {
			// retrieving or putting a result
			if parts[4] != "result" {
				return fmt.Errorf("Only results can be queried for specific ")
			}

			keyStr := serviceIdStr + "-result"
			if method == "get" {
				// retrieve the result for the given service id
				value, err := d.GetData(uuid, keyStr)
				if err != nil {
					return fmt.Errorf("No result exists")
				}
				w.Header().Set("Content-Type", "application/octet-stream")
				_, err = w.Write(value)
				if err != nil {
					return err
				}
			} else if method == "put" {
				// must be authenticated
				err = d.AuthenticateRequest(accessKeyStr, r)
				if err != nil {
					return err
				}

				data, err := dvid.DataFromPost(r, "data")
				if err != nil {
					return err
				}
				err = d.PutData(uuid, keyStr, data)
				if err != nil {
					return err
				}
			} else {
				return fmt.Errorf("Results to a service can only be GET or PUT")
			}
		}
	}

	return nil
}
