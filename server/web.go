package server

import (
	"fmt"
	"image/png"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
)

func planeHandler(w http.ResponseWriter, r *http.Request) {

}

// Handler for presentation files
func mainHandler(w http.ResponseWriter, r *http.Request) {
	filename := filepath.Join(runningService.WebClientPath, r.URL.Path)
	dvid.Log(dvid.Debug, "http request: %s -> %s\n", r.URL.Path, filename)
	http.ServeFile(w, r, filename)
}

// Handler for API commands through HTTP
func apiHandler(w http.ResponseWriter, r *http.Request) {
	const lenPath = len("/api/")
	url := r.URL.Path[lenPath:]

	var offset, size dvid.VoxelCoord
	var sx, sy int32
	parts := strings.Split(url, "/")
	uuidStr := parts[0]
	//op := parts[1]
	datatype := parts[1]
	fmt.Sscanf(parts[2], "%d,%d,%d", &offset[0], &offset[1], &offset[2])
	planeStr := parts[3]
	fmt.Sscanf(parts[4], "%d,%d", &sx, &sy)

	switch planeStr {
	case "xy":
		size = dvid.VoxelCoord{sx, sy, 1}
	case "xz":
		size = dvid.VoxelCoord{sx, 1, sy}
	case "yz":
		size = dvid.VoxelCoord{1, sx, sy}
	}

	/**
	fmt.Fprintln(w, "<h1>DVID Server API Handler ...</h1>")

	fmt.Fprintln(w, "<p>Processing", url, "</p>")
	fmt.Fprintf(w, "<p>Op: %s</p>\n", op)
	fmt.Fprintf(w, "<p>Datatype: %s</p>\n", datatype)
	fmt.Fprintf(w, "<p>Offset: %s</p>\n", offset)
	fmt.Fprintf(w, "<p>Size: %s</p>\n", size)
	**/
	// Get the image

	typeService, err := datastore.GetTypeService(datatype)
	if err != nil {
		fmt.Println("Error getting typeService:", err.Error())
		return
	}

	numBytes := int(sx*sy) * typeService.BytesPerVoxel()
	subvol := dvid.Subvolume{
		Text:          url,
		Offset:        offset,
		Size:          size,
		BytesPerVoxel: typeService.BytesPerVoxel(),
		Data:          make([]byte, numBytes, numBytes),
	}

	uuidNum, err := runningService.GetUuidFromString(uuidStr)

	versionService := datastore.NewVersionService(runningService.Service, uuidNum)

	err = typeService.GetVolume(versionService, &subvol)

	subvol.NonZeroBytes("After GetVolume()")

	// Write the image to requestor
	w.Header().Set("Content-type", "image/png")
	png.Encode(w, typeService.GetSlice(&subvol, planeStr))
}
