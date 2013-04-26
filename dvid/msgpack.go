package dvid

import (
	"bytes"
	"fmt"
	"net/http"

	"github.com/ugorji/go-msgpack"
)

const MsgPackContentType = "application/x-msgpack"

type MsgPackData []byte

func (data *MsgPackData) EncodeSubvolume(subvol *Subvolume) (err error) {
	var buffer bytes.Buffer
	enc := msgpack.NewEncoder(&buffer)
	err = enc.Encode("This is a string")
	if err != nil {
		return
	}
	*data = MsgPackData(buffer.Bytes())
	fmt.Printf("Encoded length: %d\n", len(*data))
	return
}

func (data *MsgPackData) WriteHTTP(w http.ResponseWriter) error {
	w.Header().Set("Content-type", MsgPackContentType)
	n, err := w.Write([]byte(*data))
	fmt.Printf("Wrote %d bytes in HTTP response\n", n)
	return err
}
