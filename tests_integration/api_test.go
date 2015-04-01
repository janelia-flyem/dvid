package tests_integration

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/tests"

	"github.com/janelia-flyem/dvid/datatype/labelblk"

	// Declare the data types the DVID server should support
	_ "github.com/janelia-flyem/dvid/datatype/keyvalue"
	_ "github.com/janelia-flyem/dvid/datatype/roi"
)

type tuple [4]int

var labelsROI = []tuple{
	tuple{3, 3, 2, 4}, tuple{3, 4, 2, 3}, tuple{3, 5, 3, 3},
	tuple{4, 3, 2, 5}, tuple{4, 4, 3, 4}, tuple{4, 5, 2, 4},
	//tuple{5, 2, 3, 4}, tuple{5, 3, 3, 3}, tuple{5, 4, 2, 3}, tuple{5, 5, 2, 2},
}

func labelsJSON() string {
	b, err := json.Marshal(labelsROI)
	if err != nil {
		return ""
	}
	return string(b)
}

func inroi(x, y, z int) bool {
	for _, span := range labelsROI {
		if span[0] != z {
			continue
		}
		if span[1] != y {
			continue
		}
		if span[2] > x || span[3] < x {
			continue
		}
		return true
	}
	return false
}

type labelVol struct {
	size      dvid.Point3d
	blockSize dvid.Point3d
	offset    dvid.Point3d
}

// Each voxel in volume has sequential labels in X, Y, then Z order.
// volSize = size of volume in blocks
// blockSize = size of a block in voxels
func (vol labelVol) postLabelVolume(t *testing.T, labelsName string, uuid dvid.UUID) {
	server.CreateTestInstance(t, uuid, "labelblk", labelsName, dvid.Config{})

	offset := vol.offset

	nx := vol.size[0] * vol.blockSize[0]
	ny := vol.size[1] * vol.blockSize[1]
	nz := vol.size[2] * vol.blockSize[2]

	buf := make([]byte, nx*ny*nz*8)
	var label uint64
	var x, y, z, v int32
	for z = 0; z < nz; z++ {
		for y = 0; y < ny; y++ {
			for x = 0; x < nx; x++ {
				label++
				binary.LittleEndian.PutUint64(buf[v:v+8], label)
				v += 8
			}
		}
	}
	apiStr := fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/%d_%d_%d", server.WebAPIPath,
		uuid, labelsName, nx, ny, nz, offset[0], offset[1], offset[2])
	server.TestHTTP(t, "POST", apiStr, bytes.NewBuffer(buf))
}

// the label in the test volume should just be the voxel index + 1 when iterating in ZYX order.
// The passed (x,y,z) should be world coordinates, not relative to the volume offset.
func (vol labelVol) label(x, y, z int32) uint64 {
	if x < vol.offset[0] || x >= vol.offset[0]+vol.size[0]*vol.blockSize[0] {
		return 0
	}
	if y < vol.offset[1] || y >= vol.offset[1]+vol.size[1]*vol.blockSize[1] {
		return 0
	}
	if z < vol.offset[2] || z >= vol.offset[2]+vol.size[2]*vol.blockSize[2] {
		return 0
	}
	x -= vol.offset[0]
	y -= vol.offset[1]
	z -= vol.offset[2]
	nx := vol.size[0] * vol.blockSize[0]
	nxy := nx * vol.size[1] * vol.blockSize[1]
	return uint64(z*nxy) + uint64(y*nx) + uint64(x+1)
}

type sliceTester struct {
	orient string
	width  int32
	height int32
	offset dvid.Point3d // offset of slice
}

func (s sliceTester) apiStr(uuid dvid.UUID, name string) string {
	return fmt.Sprintf("%snode/%s/%s/raw/%s/%d_%d/%d_%d_%d", server.WebAPIPath,
		uuid, name, s.orient, s.width, s.height, s.offset[0], s.offset[1], s.offset[2])
}

// make sure the given labels match what would be expected from the test volume.
func (s sliceTester) testLabel(t *testing.T, vol labelVol, img *dvid.Image) {
	data := img.Data()
	var x, y, z int32
	i := 0
	switch s.orient {
	case "xy":
		for y = 0; y < s.height; y++ {
			for x = 0; x < s.width; x++ {
				label := binary.LittleEndian.Uint64(data[i*8 : (i+1)*8])
				i++
				vx := x + s.offset[0]
				vy := y + s.offset[1]
				vz := s.offset[2]
				expected := vol.label(vx, vy, vz)
				if label != expected {
					t.Errorf("Bad label @ (%d,%d,%d): expected %d, got %d\n", vx, vy, vz, expected, label)
					return
				}
			}
		}
		return
	case "xz":
		for z = 0; z < s.height; z++ {
			for x = 0; x < s.width; x++ {
				label := binary.LittleEndian.Uint64(data[i*8 : (i+1)*8])
				i++
				vx := x + s.offset[0]
				vy := s.offset[1]
				vz := z + s.offset[2]
				expected := vol.label(vx, vy, vz)
				if label != expected {
					t.Errorf("Bad label @ (%d,%d,%d): expected %d, got %d\n", vx, vy, vz, expected, label)
					return
				}
			}
		}
		return
	case "yz":
		for z = 0; z < s.height; z++ {
			for y = 0; x < s.width; x++ {
				label := binary.LittleEndian.Uint64(data[i*8 : (i+1)*8])
				i++
				vx := s.offset[0]
				vy := y * s.offset[1]
				vz := z + s.offset[2]
				expected := vol.label(vx, vy, vz)
				if label != expected {
					t.Errorf("Bad label @ (%d,%d,%d): expected %d, got %d\n", vx, vy, vz, expected, label)
					return
				}
			}
		}
		return
	default:
		t.Fatalf("Unknown slice orientation %q\n", s.orient)
	}
}

func TestLabels(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	uuid := dvid.UUID(server.NewTestRepo(t))
	if len(uuid) < 5 {
		t.Fatalf("Bad root UUID for new repo: %s\n", uuid)
	}

	// Create a labelblk instance
	vol := labelVol{
		size:      dvid.Point3d{5, 5, 5}, // in blocks
		blockSize: dvid.Point3d{32, 32, 32},
		offset:    dvid.Point3d{32, 64, 96},
	}
	vol.postLabelVolume(t, "labels", uuid)

	// Verify XY slice.
	sliceOffset := vol.offset
	sliceOffset[0] += 51
	sliceOffset[1] += 11
	sliceOffset[2] += 23
	slice := sliceTester{"xy", 67, 83, sliceOffset}
	apiStr := slice.apiStr(uuid, "labels")
	xy := server.TestHTTP(t, "GET", apiStr, nil)
	img, format, err := dvid.ImageFromBytes(xy, labelblk.EncodeFormat(), false)
	if err != nil {
		t.Fatalf("Error on XY labels GET: %s\n", err.Error())
	}
	if format != "png" {
		t.Errorf("Expected XY labels GET to return %q image, got %q instead.\n", "png", format)
	}
	if img.NumBytes() != 67*83*8 {
		t.Errorf("Expected %d bytes from XY labelblk GET.  Got %d instead.", 160*160*8, img.NumBytes())
	}
	slice.testLabel(t, vol, img)

	// Verify XZ slice returns what we expect.
	sliceOffset = vol.offset
	sliceOffset[0] += 11
	sliceOffset[1] += 4
	sliceOffset[2] += 3
	slice = sliceTester{"xz", 67, 83, sliceOffset}
	apiStr = slice.apiStr(uuid, "labels")
	xz := server.TestHTTP(t, "GET", apiStr, nil)
	img, format, err = dvid.ImageFromBytes(xz, labelblk.EncodeFormat(), false)
	if err != nil {
		t.Fatalf("Error on XZ labels GET: %s\n", err.Error())
	}
	if format != "png" {
		t.Errorf("Expected XZ labels GET to return %q image, got %q instead.\n", "png", format)
	}
	if img.NumBytes() != 67*83*8 {
		t.Errorf("Expected %d bytes from XZ labelblk GET.  Got %d instead.", 67*83*8, img.NumBytes())
	}
	slice.testLabel(t, vol, img)

	// Verify YZ slice returns what we expect.
	sliceOffset = vol.offset
	sliceOffset[0] += 7
	sliceOffset[1] += 33
	sliceOffset[2] += 33
	slice = sliceTester{"yz", 67, 83, sliceOffset}
	apiStr = slice.apiStr(uuid, "labels")
	yz := server.TestHTTP(t, "GET", apiStr, nil)
	img, format, err = dvid.ImageFromBytes(yz, labelblk.EncodeFormat(), false)
	if err != nil {
		t.Fatalf("Error on YZ labels GET: %s\n", err.Error())
	}
	if format != "png" {
		t.Errorf("Expected YZ labels GET to return %q image, got %q instead.\n", "png", format)
	}
	if img.NumBytes() != 67*83*8 {
		t.Errorf("Expected %d bytes from YZ labelblk GET.  Got %d instead.", 67*83*8, img.NumBytes())
	}
	slice.testLabel(t, vol, img)

	// Verify 3d volume read returns original.
	/*
		var label uint64
		nx := 5
		ny := 5
		nz := 5
		blocksz := 32
		apiStr = fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/%d_%d_%d", server.WebAPIPath,
			uuid, "labels", nx*blocksz, ny*blocksz, nz*blocksz, offset[0], offset[1], offset[2])
		xyz := server.TestHTTP(t, "GET", apiStr, nil)
		if len(xyz) != 160*160*160*8 {
			t.Errorf("Expected %d bytes from 3d labelblk GET.  Got %d instead.", 160*160*160*8, len(xyz))
		}
		label = 0
		j := 0
		for z := 0; z < nz*blocksz; z++ {
			for y := 0; y < ny*blocksz; y++ {
				for x := 0; x < nx; x++ {
					label++
					for i := 0; i < blocksz; i++ {
						gotlabel := binary.LittleEndian.Uint64(xyz[j : j+8])
						if gotlabel != label {
							t.Fatalf("Bad label %d instead of expected %d at (%d,%d,%d)\n",
								gotlabel, label, x*blocksz+i, y, z)
						}
						j += 8
					}
				}
			}
		}

		// Create a new ROI instance.
			roiName := "myroi"
			server.CreateTestInstance(t, uuid, "roi", roiName, dvid.Config{})

			// Add ROI data
			apiStr = fmt.Sprintf("%snode/%s/%s/roi", server.WebAPIPath, uuid, roiName)
			server.TestHTTP(t, "POST", apiStr, bytes.NewBufferString(labelsJSON()))

			// Post updated labels without ROI.
			p := make([]byte, 8*blocksz)
			payload := new(bytes.Buffer)
			label = 200000
			for z := 0; z < nz*blocksz; z++ {
				for y := 0; y < ny*blocksz; y++ {
					for x := 0; x < nx; x++ {
						label++
						for i := 0; i < blocksz; i++ {
							binary.LittleEndian.PutUint64(p[i*8:(i+1)*8], label)
						}
						n, err := payload.Write(p)
						if n != 8*blocksz {
							t.Fatalf("Could not write test data: %d bytes instead of %d\n", n, 8*blocksz)
						}
						if err != nil {
							t.Fatalf("Could not write test data: %s\n", err.Error())
						}
					}
				}
			}
			apiStr = fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/32_64_96", server.WebAPIPath,
				uuid, "labels", nx*blocksz, ny*blocksz, nz*blocksz)
			server.TestHTTP(t, "POST", apiStr, payload)

			// Verify 3d volume read returns modified data.
			apiStr = fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/32_64_96", server.WebAPIPath,
				uuid, "labels", nx*blocksz, ny*blocksz, nz*blocksz)
			xyz = server.TestHTTP(t, "GET", apiStr, nil)
			if len(xyz) != 160*160*160*8 {
				t.Errorf("Expected %d bytes from 3d labelblk GET.  Got %d instead.", 160*160*160*8, len(xyz))
			}
			label = 200000
			j = 0
			for z := 0; z < nz*blocksz; z++ {
				for y := 0; y < ny*blocksz; y++ {
					for x := 0; x < nx; x++ {
						label++
						for i := 0; i < blocksz; i++ {
							gotlabel := binary.LittleEndian.Uint64(xyz[j : j+8])
							if gotlabel != label {
								t.Fatalf("Bad label %d instead of expected modified %d at (%d,%d,%d)\n",
									gotlabel, label, x*blocksz+i, y, z)
							}
							j += 8
						}
					}
				}
			}

			// TODO - Use the ROI to retrieve a 2d xy image.

			// TODO - Make sure we aren't getting labels back in non-ROI points.

			// Post again but now with ROI
			payload = new(bytes.Buffer)
			label = 400000
			for z := 0; z < nz*blocksz; z++ {
				for y := 0; y < ny*blocksz; y++ {
					for x := 0; x < nx; x++ {
						label++
						for i := 0; i < blocksz; i++ {
							binary.LittleEndian.PutUint64(p[i*8:(i+1)*8], label)
						}
						n, err := payload.Write(p)
						if n != 8*blocksz {
							t.Fatalf("Could not write test data: %d bytes instead of %d\n", n, 8*blocksz)
						}
						if err != nil {
							t.Fatalf("Could not write test data: %s\n", err.Error())
						}
					}
				}
			}
			apiStr = fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/32_64_96?roi=%s", server.WebAPIPath,
				uuid, "labels", nx*blocksz, ny*blocksz, nz*blocksz, roiName)
			server.TestHTTP(t, "POST", apiStr, payload)

			// Verify ROI masking on GET.
			apiStr = fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/32_64_96?roi=%s", server.WebAPIPath,
				uuid, "labels", nx*blocksz, ny*blocksz, nz*blocksz, roiName)
			xyz2 := server.TestHTTP(t, "GET", apiStr, nil)
			if len(xyz) != 160*160*160*8 {
				t.Errorf("Expected %d bytes from 3d labelblk GET.  Got %d instead.", 160*160*160*8, len(xyz))
			}
			var newlabel uint64 = 400000
			var oldlabel uint64 = 200000
			j = 0
			offsetx := 16
			offsety := 48
			offsetz := 70
			for z := 0; z < nz*blocksz; z++ {
				voxz := z + offsetz
				blockz := voxz / int(imageblk.DefaultBlockSize)
				for y := 0; y < ny*blocksz; y++ {
					voxy := y + offsety
					blocky := voxy / int(imageblk.DefaultBlockSize)
					for x := 0; x < nx; x++ {
						newlabel++
						oldlabel++
						voxx := x*blocksz + offsetx
						for i := 0; i < blocksz; i++ {
							blockx := voxx / int(imageblk.DefaultBlockSize)
							gotlabel := binary.LittleEndian.Uint64(xyz2[j : j+8])
							if inroi(blockx, blocky, blockz) {
								if gotlabel != newlabel {
									t.Fatalf("Got label %d instead of in-ROI label %d at (%d,%d,%d)\n",
										gotlabel, newlabel, voxx, voxy, voxz)
								}
							} else {
								if gotlabel != 0 {
									t.Fatalf("Got label %d instead of 0 at (%d,%d,%d) outside ROI\n",
										gotlabel, voxx, voxy, voxz)
								}
							}
							j += 8
							voxx++
						}
					}
				}
			}

			// Verify everything in mask is new and everything out of mask is old, and everything in mask
			// is new.
			apiStr = fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/32_64_96", server.WebAPIPath,
				uuid, "labels", nx*blocksz, ny*blocksz, nz*blocksz)
			xyz2 = server.TestHTTP(t, "GET", apiStr, nil)
			if len(xyz) != 160*160*160*8 {
				t.Errorf("Expected %d bytes from 3d labelblk GET.  Got %d instead.", 160*160*160*8, len(xyz))
			}
			newlabel = 400000
			oldlabel = 200000
			j = 0
			for z := 0; z < nz*blocksz; z++ {
				voxz := z + offsetz
				blockz := voxz / int(imageblk.DefaultBlockSize)
				for y := 0; y < ny*blocksz; y++ {
					voxy := y + offsety
					blocky := voxy / int(imageblk.DefaultBlockSize)
					for x := 0; x < nx; x++ {
						newlabel++
						oldlabel++
						voxx := x*blocksz + offsetx
						for i := 0; i < blocksz; i++ {
							blockx := voxx / int(imageblk.DefaultBlockSize)
							gotlabel := binary.LittleEndian.Uint64(xyz2[j : j+8])
							if inroi(blockx, blocky, blockz) {
								if gotlabel != newlabel {
									t.Fatalf("Got label %d instead of in-ROI label %d at (%d,%d,%d)\n",
										gotlabel, newlabel, voxx, voxy, voxz)
								}
							} else {
								if gotlabel != oldlabel {
									t.Fatalf("Got label %d instead of label %d at (%d,%d,%d) outside ROI\n",
										gotlabel, oldlabel, voxx, voxy, voxz)
								}
							}
							j += 8
							voxx++
						}
					}
				}
			}
	*/
}

func TestCommitAndBranch(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	apiStr := fmt.Sprintf("%srepos", server.WebAPIPath)
	r := server.TestHTTP(t, "POST", apiStr, nil)
	var jsonResp map[string]interface{}

	if err := json.Unmarshal(r, &jsonResp); err != nil {
		t.Fatalf("Unable to unmarshal repo creation response: %s\n", string(r))
	}
	v, ok := jsonResp["root"]
	if !ok {
		t.Fatalf("No 'root' metadata returned: %s\n", string(r))
	}
	uuidStr, ok := v.(string)
	if !ok {
		t.Fatalf("Couldn't cast returned 'root' data (%v) into string.\n", v)
	}
	uuid := dvid.UUID(uuidStr)

	// Shouldn't be able to create branch on open node.
	branchReq := fmt.Sprintf("%snode/%s/branch", server.WebAPIPath, uuid)
	server.TestBadHTTP(t, "POST", branchReq, nil)

	// Add a keyvalue instance.
	server.CreateTestInstance(t, uuid, "keyvalue", "mykv", dvid.Config{})

	// Commit it.
	payload := bytes.NewBufferString(`{"note": "This is my test commit", "log": ["line1", "line2", "some more stuff in a line"]}`)
	apiStr = fmt.Sprintf("%snode/%s/commit", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", apiStr, payload)

	// Make sure committed nodes can only be read.
	// We shouldn't be able to write to keyvalue..
	keyReq := fmt.Sprintf("%snode/%s/mykv/key/foo", server.WebAPIPath, uuid)
	server.TestBadHTTP(t, "POST", keyReq, bytes.NewBufferString("some data"))

	// Should be able to create branch now that we've committed parent.
	respData := server.TestHTTP(t, "POST", branchReq, nil)
	resp := struct {
		Child dvid.UUID `json:"child"`
	}{}
	if err := json.Unmarshal(respData, &resp); err != nil {
		t.Errorf("Expected 'child' JSON response.  Got %s\n", string(respData))
	}

	// We should be able to write to that keyvalue now in the child.
	keyReq = fmt.Sprintf("%snode/%s/mykv/key/foo", server.WebAPIPath, resp.Child)
	server.TestHTTP(t, "POST", keyReq, bytes.NewBufferString("some data"))
}
