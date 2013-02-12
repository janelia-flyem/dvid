package grayscale8

import (
	"fmt"
	"github.com/janelia-flyem/dvid/datatype"
)

const repoUrl = "github.com/janelia-flyem/dvid/datatype/grayscale8"

type Format struct {
	datatype.Format
}

func init() {
	datatype.RegisterFormat(&Format{
		datatype.Format{
			Name:        "grayscale8",
			Url:         repoUrl,
			IsolateData: true,
		},
	})
}

func (format *Format) Add(datastoreDir, filenameGlob, uuidString, params string) (err error) {
	// Get the plane coordinates for images.
	//coords := make(map[string]int, 3)

	fmt.Println("Datastore Dir: ", datastoreDir)
	fmt.Println("filename Glob: ", filenameGlob)
	fmt.Println("uuid string: ", uuidString)
	fmt.Println("params: ", params)
	return
}
