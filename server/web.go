package server

import (
	"fmt"
	"image/png"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"

	"code.google.com/p/go.net/websocket"
)

func socketHandler(c *websocket.Conn) {
	var s string
	fmt.Fscan(c, &s)
	dvid.Log(dvid.Debug, "socket received: %s\n", s)
	fmt.Fprint(c, "DVID received socket communication:", s, "\n")
}

func planeHandler(w http.ResponseWriter, r *http.Request) {

}

// Handler for presentation files
func mainHandler(w http.ResponseWriter, r *http.Request) {
	filename := filepath.Join(runningService.WebClientPath, r.URL.Path)
	dvid.Log(dvid.Debug, "http request: %s -> %s\n", r.URL.Path, filename)
	http.ServeFile(w, r, filename)
}

func badRequest(w http.ResponseWriter, r *http.Request, err string) {
	errorMsg := fmt.Sprintf("ERROR using REST API: %s (%s).", err, r.URL.Path)
	errorMsg += "  Use 'dvid help' to get proper API request format.\n"
	dvid.Error(errorMsg)
	http.Error(w, errorMsg, http.StatusBadRequest)
}

// Handler for API commands through HTTP
func apiHandler(w http.ResponseWriter, r *http.Request) {
	const lenPath = len(RestApiPath)
	url := r.URL.Path[lenPath:]

	// Break URL request into arguments
	parts := strings.Split(url, "/")
	if len(parts) != 5 {
		badRequest(w, r, "Poorly formed request")
		return
	}

	uuidNum, err := runningService.GetUuidFromString(parts[0])
	versionService := datastore.NewVersionService(runningService.Service, uuidNum)

	// Given a data type abbreviated name (e.g., 'grayscale8'), get the type's service,
	// which is an interface for doing operations
	datatype := parts[1]
	typeService, err := datastore.GetTypeService(datatype)
	if err != nil {
		fmt.Println("Error getting typeService:", err.Error())
		return
	}

	volumeStr := strings.ToLower(parts[2])
	offsetStr := dvid.PointStr(parts[3])
	sizeStr := dvid.PointStr(parts[4])
	var normalStr dvid.VectorStr
	if len(parts) > 5 {
		normalStr = dvid.VectorStr(parts[5])
	}

	offset, err := offsetStr.VoxelCoord()
	if err != nil {
		badRequest(w, r, fmt.Sprintf("Unable to parse Offset as 'x,y,z', got '%s'", offsetStr))
		return
	}

	// Package Subvolume requests
	if volumeStr == "vol" {
		size, err := sizeStr.VoxelCoord()
		if err != nil {
			badRequest(w, r, fmt.Sprintf("Unable to parse Size as 'x,y,z', got '%s'", sizeStr))
			return
		}
		numBytes := size.Mag() * typeService.BytesPerVoxel()
		subvol := dvid.Subvolume{
			Text:          url,
			Offset:        offset,
			Size:          size,
			BytesPerVoxel: typeService.BytesPerVoxel(),
			Data:          make([]byte, numBytes, numBytes),
		}
		cmd := 

		err = typeService.GetVolume(versionService, &subvol)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// TODO -- Return a subvolume using some kind of container

	} else { // Package Slice requests

		size, err := sizeStr.Point2d()
		if err != nil {
			badRequest(w, r, fmt.Sprintf("Unable to parse Size as 'x,y', got '%s'", sizeStr))
			return
		}

		var slice dvid.Slice
		switch strings.ToLower(parts[3]) {
		case "xy":
			slice = dvid.NewSliceXY(&offset, &size)
		case "xz":
			slice = dvid.NewSliceXZ(&offset, &size)
		case "yz":
			slice = dvid.NewSliceYZ(&offset, &size)
		case "arb":
			normal, err := normalStr.Vector3d()
			if err != nil {
				badRequest(w, r, fmt.Sprintf("Unable to parse normal as 'x,y,z', got '%s'",
					normalStr))
				return
			}
			slice = dvid.NewSliceArb(&offset, &size, &normal)
		default:
			badRequest(w, r, fmt.Sprintf("Illegal slice plane specification (%s)", parts[3]))
			return
		}

		// Write the image to requestor
		w.Header().Set("Content-type", "image/png")
		png.Encode(w, typeService.GetSlice(&slice))
	}
}
