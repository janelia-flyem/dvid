package service

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"github.com/janelia-flyem/dvid/datastore"
	"io"
	"log"
	"mime/multipart"
	"net/http"
)

const ServiceContract = `
API for calling the helloworld service

External API
{
    "message" : "<hello world message>" [string]
    "interface-version" : "0.1" [string]
}

Internal API
{
    "server-path" : "<location of DVID server>" [string]
    "uuid" : "<DVID node UUID for dataset>" [string]
    "callback" : "<URI for service id>" [string]
    "status" : "<state of service>" [string] (initially "not started") 
    "access-key" : "<random key to post to callback>" [string]
}

Results Posted To Callback

(JSON format)
{
    "hello" : "<hello world message>"
}
`

// implement ServiceExectuor interface
type MessageEcho struct{}

// read json and write back to the server
func (s *MessageEcho) RunService(jsonStr string) {
	// catch any panics that arise from service
	defer func() {
		if err := recover(); err != nil {
			log.Printf("Error in service")
		}
	}()

	// read json input for service
	jsonData := make(map[string]interface{})
	err := json.Unmarshal([]byte(jsonStr), &jsonData)
	if err != nil {
		return
	}

	// retrieve message passed in
	message, ok := jsonData["message"]
	if !ok {
		return
	}

	// make a status message and result message to be put for the specific service callback id
	resultJSON, _ := json.Marshal(map[string]string{"hello": message.(string)})
	statusJSON, _ := json.Marshal(map[string]string{"status": "completed"})

	serverPath, ok := jsonData["server-path"]
	if !ok {
		return
	}
	callback, ok := jsonData["callback"]
	if !ok {
		return
	}
	accessKey, ok := jsonData["access-key"]
	if !ok {
		return
	}

	// callback paths
	statusPath := "http://" + serverPath.(string) + callback.(string)
	resultPath := statusPath + "/result"

	client := &http.Client{}

	// multipart form -- NOT WORKING -- want to take resultJSON and add it to "data"
	tempResultBuffer := bytes.NewBuffer(resultJSON)
	resultBuffer := &bytes.Buffer{}
	writer := multipart.NewWriter(resultBuffer)
	part, err := writer.CreateFormFile("data", "result.json")
	if err != nil {
		return
	}
	_, err = io.Copy(part, tempResultBuffer)
	if err != nil {
		return
	}
	err = writer.Close()
	if err != nil {
		return
	}

	// put a message result for the service call
	req, err := http.NewRequest("PUT", resultPath, resultBuffer)
	req.SetBasicAuth(accessKey.(string), "")
	resp, err := client.Do(req)
	if err != nil {
		return
	}

	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}

	// put a status message for the service call
	req, err = http.NewRequest("PUT", statusPath, bytes.NewBuffer(statusJSON))
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(accessKey.(string), "")
	resp, err = client.Do(req)

	if err != nil {
		return
	}

	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
}

func init() {
	gob.Register(&MessageEcho{})
	MessageEchoService := &MessageEcho{}
	helloworld := NewDatatype(MessageEchoService, ServiceContract)
	helloworld.DatatypeID = &datastore.DatatypeID{
		Name:    "helloworld",
		Url:     "github.com/janelia-flyem/dvid/service/helloworld.go",
		Version: "0.1",
	}
	datastore.RegisterDatatype(helloworld)
}
