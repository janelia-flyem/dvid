package grayscale8

import (
	"github.com/janelia-flyem/dvid/datatype"
)

const repoUrl = "github.com/janelia-flyem/dvid/datatype/grayscale8"

func init() {
	withinBlock := true
	datatype.RegisterFormat("grayscale8", repoUrl, withinBlock)
}
