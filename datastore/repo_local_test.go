// +build !clustered,!gcloud

package datastore

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/janelia-flyem/dvid/dvid"
)

func TestRepoGobEncoding(t *testing.T) {
	uuid := dvid.UUID("19b87f38f873481b9f3ac688877dff0d")
	versionID := dvid.VersionID(23)
	repoID := dvid.RepoID(13)

	repo := newRepo(uuid, versionID, repoID, "foobar")
	repo.alias = "just some alias"
	repo.log = []string{
		"Did this",
		"Then that",
		"And the other thing",
	}
	repo.properties = map[string]interface{}{
		"foo": 42,
		"bar": "some string",
		"baz": []int{3, 9, 7},
	}

	encoding, err := repo.GobEncode()
	if err != nil {
		t.Fatalf("Could not encode repo: %v\n", err)
	}
	received := repoT{}
	if err = received.GobDecode(encoding); err != nil {
		t.Fatalf("Could not decode repo: %v\n", err)
	}

	// Did we serialize OK
	repo.dag = nil
	received.dag = nil
	if len(received.properties) != 3 {
		t.Errorf("Repo Gob messed up properties: %v\n", received.properties)
	}
	foo, ok := received.properties["foo"]
	if !ok || foo != 42 {
		t.Errorf("Repo Gob messed up properties: %v\n", received.properties)
	}
	bar, ok := received.properties["bar"]
	if !ok || bar != "some string" {
		t.Errorf("Repo Gob messed up properties: %v\n", received.properties)
	}
	baz, ok := received.properties["baz"]
	if !ok || !reflect.DeepEqual(baz, []int{3, 9, 7}) {
		t.Errorf("Repo Gob messed up properties: %v\n", received.properties)
	}
	repo.properties = nil
	received.properties = nil
	if !reflect.DeepEqual(*repo, received) {
		t.Fatalf("Repo Gob messed up:\nOriginal: %v\nReceived: %v\n", *repo, received)
	}
}

func makeTestVersions(t *testing.T) {
	root, err := NewRepo("test repo", "test repo description", nil, "")
	if err != nil {
		t.Fatal(err)
	}

	if err := Commit(root, "root node", nil); err != nil {
		t.Fatal(err)
	}

	child1, err := NewVersion(root, "note describing child 1", "", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := Commit(child1, "child 1", nil); err != nil {
		t.Fatal(err)
	}

	// Test ability to set UUID of child
	assignedUUID := dvid.UUID("0c8bc973dba74729880dd1bdfd8d0c5e")
	child2, err := NewVersion(root, "note describing child 2", "child2", &assignedUUID)
	if err != nil {
		t.Fatal(err)
	}

	log2 := []string{"This is line 1 of log", "This is line 2 of log", "Last line for multiline log"}
	if err := Commit(child2, "child 2 assigned", log2); err != nil {
		t.Fatal(err)
	}

	// Make uncommitted child 3
	child3, err := NewVersion(root, "note describing child 3", "child3", nil)
	if err != nil {
		t.Fatal(err)
	}
	nodelog := []string{`My first node-level log line.!(;#)}`, "Second line is here!!!"}
	if err := AddToNodeLog(child3, nodelog); err != nil {
		t.Fatal(err)
	}
}

func TestRepoPersistence(t *testing.T) {
	OpenTest()

	makeTestVersions(t)

	// Save this metadata
	jsonBytes, err := MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	jsonStr := string(jsonBytes)
	mutidJSON := fmt.Sprintf(`"MutationID":%d`, InitialMutationID)
	mutidPos := strings.Index(jsonStr, mutidJSON)
	if mutidPos < 0 {
		t.Fatalf("Expected repo JSON to include MutationID set to %d, but didn't find it\n", InitialMutationID)
	}
	jsonStr = jsonStr[:mutidPos] + jsonStr[mutidPos+len(mutidJSON):]

	mutidJSON = fmt.Sprintf(`"SavedMutationID":%d`, InitialMutationID+StrideMutationID)
	mutidPos = strings.Index(jsonStr, mutidJSON)
	if mutidPos < 0 {
		t.Fatalf("Expected repo JSON to include SavedMutationID set to %d, but didn't find it\n", InitialMutationID+StrideMutationID)
	}
	jsonStr = jsonStr[:mutidPos] + jsonStr[mutidPos+len(mutidJSON):]

	// Shutdown and restart.
	CloseReopenTest()
	defer CloseTest()

	// Check if metadata is same
	jsonBytes2, err := MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	jsonStr2 := string(jsonBytes2)
	mutidJSON = fmt.Sprintf(`"MutationID":%d`, InitialMutationID+StrideMutationID)
	mutidPos = strings.Index(jsonStr2, mutidJSON)
	if mutidPos < 0 {
		t.Fatalf("Expected repo JSON to include MutationID set to %d, but didn't find it\n", InitialMutationID)
	}
	jsonStr2 = jsonStr2[:mutidPos] + jsonStr2[mutidPos+len(mutidJSON):]

	mutidJSON = fmt.Sprintf(`"SavedMutationID":%d`, InitialMutationID+2*StrideMutationID)
	mutidPos = strings.Index(jsonStr2, mutidJSON)
	if mutidPos < 0 {
		t.Fatalf("Expected repo JSON to include SavedMutationID set to %d, but didn't find it\n", InitialMutationID+2*StrideMutationID)
	}
	jsonStr2 = jsonStr2[:mutidPos] + jsonStr2[mutidPos+len(mutidJSON):]

	if jsonStr != jsonStr2 {
		t.Errorf("\nRepo metadata JSON changes on close/reopen:\n\nOld:\n%s\n\nNew:\n%s\n", jsonStr, jsonStr2)
	}
}

// Make sure each new repo has a different local ID.
func TestNewRepoDifferent(t *testing.T) {
	OpenTest()
	defer CloseTest()

	root1, err := NewRepo("test repo 1", "test repo 1 description", nil, "")
	if err != nil {
		t.Fatal(err)
	}

	root2, err := NewRepo("test repo 2", "test repo 2 description", nil, "")
	if err != nil {
		t.Fatal(err)
	}

	// Delve down into private methods to make sure internal IDs are different.
	repo1, err := manager.repoFromUUID(root1)
	if err != nil {
		t.Fatal(err)
	}
	repo2, err := manager.repoFromUUID(root2)
	if err != nil {
		t.Fatal(err)
	}
	if root1 == root2 {
		t.Errorf("New repos share uuid: %s\n", root1)
	}
	if repo1.id == repo2.id {
		t.Errorf("New repos share repo id: %d\n", repo1.id)
	}
	if repo1.version == repo2.version {
		t.Errorf("New repos share version id: %d\n", repo1.version)
	}
	if repo1.alias == repo2.alias {
		t.Errorf("New repos share alias: %s\n", repo1.alias)
	}
}

func TestUUIDAssignment(t *testing.T) {
	OpenTest()
	defer CloseTest()

	uuidStr1 := "de305d5475b4431badb2eb6b9e546014"
	myuuid := dvid.UUID(uuidStr1)
	root, err := NewRepo("test repo", "test repo description", &myuuid, "")
	if err != nil {
		t.Fatal(err)
	}
	if root != myuuid {
		t.Errorf("Assigned root UUID %q != created root UUID %q\n", myuuid, root)
	}

	// Check if branches can also have assigned UUIDs
	if err := Commit(root, "root node", nil); err != nil {
		t.Fatal(err)
	}
	uuidStr2 := "8fa05d5475b4431badb2eb6b9e0123014"
	myuuid2 := dvid.UUID(uuidStr2)
	child, err := NewVersion(myuuid, "note describing uuid2", "", &myuuid2)
	if err != nil {
		t.Fatal(err)
	}
	if child != myuuid2 {
		t.Errorf("Assigned child UUID %q != created child UUID %q\n", myuuid2, child)
	}

	// Make sure we can lookup assigned UUIDs
	uuid, _, err := MatchingUUID(uuidStr1[:10])
	if err != nil {
		t.Errorf("Error matching UUID fragment %s: %v\n", uuidStr1[:10], err)
	}
	if uuid != myuuid {
		t.Errorf("Error getting back correct UUID %s from %s\n", myuuid, uuid)
	}
}

func equalVersionSlices(a, b []dvid.VersionID) bool {
	if len(a) != len(b) {
		return false
	}
	mapA := make(map[dvid.VersionID]struct{}, len(a))
	for _, v := range a {
		mapA[v] = struct{}{}
	}
	for _, v := range b {
		if _, found := mapA[v]; !found {
			return false
		}
	}
	return true
}

func TestDAGDup(t *testing.T) {
	nodes := []nodeT{
		{
			uuid:     dvid.UUID("uuid3"),
			version:  3,
			locked:   true,
			children: []dvid.VersionID{5},
		},
		{
			version:  5,
			locked:   true,
			parents:  []dvid.VersionID{3},
			children: []dvid.VersionID{7, 8, 9, 10},
		},
		{
			uuid:     dvid.UUID("uuid7"),
			version:  7,
			locked:   true,
			parents:  []dvid.VersionID{5},
			children: []dvid.VersionID{11, 13},
		},
		{
			version: 8,
			parents: []dvid.VersionID{5},
			locked:  true,
		},
		{
			version: 9,
			parents: []dvid.VersionID{5},
			locked:  false,
		},
		{
			branch:   "branch10",
			version:  10,
			locked:   true,
			parents:  []dvid.VersionID{5},
			children: []dvid.VersionID{14},
		},
		{
			version: 11,
			parents: []dvid.VersionID{7},
			locked:  true,
		},
		{
			version: 13,
			parents: []dvid.VersionID{7},
			locked:  false,
		},
		{
			version:  14,
			locked:   false,
			parents:  []dvid.VersionID{10},
			children: []dvid.VersionID{15},
		},
		{
			branch:  "branch15",
			version: 15,
			locked:  false,
			parents: []dvid.VersionID{14},
		},
	}
	dag := new(dagT)
	dag.root = dvid.UUID("uuid3")
	dag.rootV = 3
	dag.nodes = make(map[dvid.VersionID]*nodeT, len(nodes))
	for i, node := range nodes {
		dag.nodes[node.version] = &(nodes[i])
	}
	okVersions := map[dvid.VersionID]struct{}{
		5:  struct{}{},
		7:  struct{}{},
		10: struct{}{},
		11: struct{}{},
		15: struct{}{},
	}
	dup := dag.duplicate(okVersions)
	expected := map[dvid.VersionID]nodeT{
		5: {
			version:  5,
			locked:   true,
			children: []dvid.VersionID{7, 10},
		},
		7: {
			uuid:     dvid.UUID("uuid7"),
			version:  7,
			locked:   true,
			parents:  []dvid.VersionID{5},
			children: []dvid.VersionID{11},
		},
		10: {
			branch:   "branch10",
			version:  10,
			locked:   true,
			parents:  []dvid.VersionID{5},
			children: []dvid.VersionID{15},
		},
		11: {
			version: 11,
			parents: []dvid.VersionID{7},
			locked:  true,
		},
		15: {
			branch:  "branch15",
			version: 15,
			locked:  false,
			parents: []dvid.VersionID{10},
		},
	}
	for v, node := range dup.nodes {
		if _, found := expected[v]; !found {
			t.Errorf("Expected to find version %d but not in dup nodes: %v\n", v, dup.nodes)
		} else {
			if !equalVersionSlices(node.children, expected[v].children) ||
				!equalVersionSlices(node.parents, expected[v].parents) ||
				node.branch != expected[v].branch ||
				node.uuid != expected[v].uuid ||
				node.version != expected[v].version ||
				node.locked != expected[v].locked {
				t.Errorf("Expected version %d to agree but didn't.\nDup: %v\nExp: %v", v, *node, expected[v])
			}
		}
	}
}

type hemiNode struct {
	VersionID dvid.VersionID
	UUID      string
	Parents   []dvid.VersionID
	Children  []dvid.VersionID
	Locked    bool
	Branch    string
	Note      string
}

func TestHemibrainDAG(t *testing.T) {
	data := make(map[string]hemiNode)
	if err := json.Unmarshal([]byte(hemibrainDAG), &data); err != nil {
		t.Fatalf("problem unmarshaling test repo info: %v\n", err)
	}
	dag := new(dagT)
	dag.root = dvid.UUID("28841c8277e044a7b187dda03e18da13")
	dag.rootV = 1
	dag.nodes = make(map[dvid.VersionID]*nodeT, len(data))
	nodes := make([]nodeT, len(data))
	num := 0
	for uuid, node := range data {
		if uuid != node.UUID {
			t.Fatalf("unexpected discrepancy in test repo info: %s != %s\n", uuid, node.UUID)
		}
		nodes[num] = nodeT{
			uuid:     dvid.UUID(node.UUID),
			version:  node.VersionID,
			locked:   node.Locked,
			children: node.Children,
			parents:  node.Parents,
			branch:   node.Branch,
			note:     node.Note,
		}
		dag.nodes[node.VersionID] = &(nodes[num])
		num++
	}

	okVersions := map[dvid.VersionID]struct{}{
		1:  struct{}{}, // 28841c8277e044a7b187dda03e18da13
		20: struct{}{}, // a0dfa4f260244946b7abbab4dcd2cc86
		42: struct{}{}, // 52a13328874c4bb7b15dc4280da26576
	}
	dup := dag.duplicate(okVersions)
	expected := map[dvid.VersionID]nodeT{
		1: {
			uuid:     dvid.UUID("28841c8277e044a7b187dda03e18da13"),
			version:  1,
			locked:   true,
			children: []dvid.VersionID{20},
		},
		20: {
			uuid:     dvid.UUID("a0dfa4f260244946b7abbab4dcd2cc86"),
			version:  20,
			locked:   true,
			parents:  []dvid.VersionID{1},
			children: []dvid.VersionID{42},
		},
		42: {
			uuid:     dvid.UUID("52a13328874c4bb7b15dc4280da26576"),
			version:  42,
			locked:   true,
			parents:  []dvid.VersionID{20},
			children: []dvid.VersionID{},
		},
	}
	for v, node := range dup.nodes {
		if expectedNode, found := expected[v]; !found {
			t.Errorf("Expected to find version %d but not in dup nodes: %v\n", v, dup.nodes)
		} else {
			if !equalVersionSlices(node.children, expectedNode.children) ||
				!equalVersionSlices(node.parents, expectedNode.parents) ||
				node.branch != expectedNode.branch ||
				node.uuid != expectedNode.uuid ||
				node.version != expectedNode.version ||
				node.locked != expectedNode.locked {
				t.Errorf("Expected version %d to agree but didn't.\nDup: %s\nExp: %s", v, node, &expectedNode)
			}
		}
	}
}

// actual data from Hemibrain dataset DAG
var hemibrainDAG = `{
	"0b0b540131954edaac208c93b0355629": {
		"Branch": "",
		"Note": "production from 2019-06-27 to 2019-07-21",
		"Log": [],
		"UUID": "0b0b540131954edaac208c93b0355629",
		"VersionID": 24,
		"Locked": true,
		"Parents": [23],
		"Children": [25],
		"Created": "2019-06-27T23:57:49.317420007-04:00",
		"Updated": "2019-07-21T13:32:10.957204972-04:00"
	},
	"0c4c21a58f8049bda364b90506b266a2": {
		"Branch": "",
		"Note": "production from 2019-09-02 to 2019-09-12 after completion of BU neuron tracings",
		"Log": [],
		"UUID": "0c4c21a58f8049bda364b90506b266a2",
		"VersionID": 27,
		"Locked": true,
		"Parents": [26],
		"Children": [28],
		"Created": "2019-09-02T19:32:52.377490883-04:00",
		"Updated": "2019-09-12T00:37:22.53459892-04:00"
	},
	"0c6f54d9677f4d3181eedfb272b1c0b0": {
		"Branch": "",
		"Note": "production from 2019-07-21 to 2019-08-11",
		"Log": [],
		"UUID": "0c6f54d9677f4d3181eedfb272b1c0b0",
		"VersionID": 25,
		"Locked": true,
		"Parents": [24],
		"Children": [26],
		"Created": "2019-07-21T13:32:10.957202176-04:00",
		"Updated": "2019-08-11T15:00:45.652039824-04:00"
	},
	"137de8281d33406aa6d7b5b1c90db1e0": {
		"Branch": "",
		"Note": "production until start of full mutation log 2019-01-17",
		"Log": [],
		"UUID": "137de8281d33406aa6d7b5b1c90db1e0",
		"VersionID": 10,
		"Locked": true,
		"Parents": [9],
		"Children": [11],
		"Created": "2018-12-16T17:00:53.556070227-05:00",
		"Updated": "2019-01-16T20:50:58.170919017-05:00"
	},
	"1d9082dc99a8469d9a1b5abf9cf807c8": {
		"Branch": "",
		"Note": "Production on 2019-11-18 before 2nd attempt at segmentation mask",
		"Log": [],
		"UUID": "1d9082dc99a8469d9a1b5abf9cf807c8",
		"VersionID": 34,
		"Locked": true,
		"Parents": [33],
		"Children": [35],
		"Created": "2019-11-18T02:00:47.870468874-05:00",
		"Updated": "2019-11-18T18:11:59.124590053-05:00"
	},
	"250ed0bcd59745dabd1c0b6ae7c4105d": {
		"Branch": "",
		"Note": "production from 2019-09-12 (after completion of BU neuron tracings)",
		"Log": [],
		"UUID": "250ed0bcd59745dabd1c0b6ae7c4105d",
		"VersionID": 28,
		"Locked": true,
		"Parents": [27],
		"Children": [29],
		"Created": "2019-09-12T00:37:22.534589266-04:00",
		"Updated": "2019-09-26T23:40:14.014934122-04:00"
	},
	"28841c8277e044a7b187dda03e18da13": {
		"Branch": "",
		"Note": "flattened and hacked version",
		"Log": [],
		"UUID": "28841c8277e044a7b187dda03e18da13",
		"VersionID": 1,
		"Locked": true,
		"Parents": [],
		"Children": [2],
		"Created": "2018-10-12T21:10:53.049157363-04:00",
		"Updated": "2018-10-15T12:06:14.918691733-04:00"
	},
	"28e669994b234683ae1e0d8eae414d6b": {
		"Branch": "",
		"Note": "production from 2019-08-11 to 2019-09-02",
		"Log": [],
		"UUID": "28e669994b234683ae1e0d8eae414d6b",
		"VersionID": 26,
		"Locked": true,
		"Parents": [25],
		"Children": [27],
		"Created": "2019-08-11T15:00:45.652037823-04:00",
		"Updated": "2019-09-02T19:32:52.37749967-04:00"
	},
	"2bbb370ff2c446cca1b4a7f394ba9df7": {
		"Branch": "",
		"Note": "Frankenbody main supervoxel 106979579 split into 86k supervoxels within neuropil, cleaves to follow",
		"Log": [],
		"UUID": "2bbb370ff2c446cca1b4a7f394ba9df7",
		"VersionID": 40,
		"Locked": true,
		"Parents": [39],
		"Children": [41],
		"Created": "2019-12-05T20:30:08.567949655-05:00",
		"Updated": "2019-12-05T23:01:03.766718787-05:00"
	},
	"305b514e13d0411c8fe6c789935e7030": {
		"Branch": "",
		"Note": "production from 2019-01-17 until 2019-02-26, before frankenbody patch",
		"Log": [],
		"UUID": "305b514e13d0411c8fe6c789935e7030",
		"VersionID": 11,
		"Locked": true,
		"Parents": [10],
		"Children": [12],
		"Created": "2019-01-16T20:50:58.170917058-05:00",
		"Updated": "2019-02-27T03:54:40.30651288-05:00"
	},
	"3230481be8384c64bdab639318f4b15d": {
		"Branch": "",
		"Note": "proofreading until main freeze on 2019-01-15 for publication",
		"Log": [],
		"UUID": "3230481be8384c64bdab639318f4b15d",
		"VersionID": 47,
		"Locked": true,
		"Parents": [46],
		"Children": [48],
		"Created": "2020-01-12T00:58:31.500712784-05:00",
		"Updated": "2020-01-15T21:04:05.266801648-05:00"
	},
	"34d7acb0167d438baa85c47ea0a739d4": {
		"Branch": "",
		"Note": "v0.9 release. Production until 2019-11-09",
		"Log": [],
		"UUID": "34d7acb0167d438baa85c47ea0a739d4",
		"VersionID": 32,
		"Locked": true,
		"Parents": [31],
		"Children": [33],
		"Created": "2019-10-27T22:48:01.892266145-04:00",
		"Updated": "2019-11-09T23:57:47.132208229-05:00"
	},
	"36b02a56f02042889aeb5260f81751a8": {
		"Branch": "",
		"Note": "production from 2019-05-10 to 2019-05-24",
		"Log": [],
		"UUID": "36b02a56f02042889aeb5260f81751a8",
		"VersionID": 22,
		"Locked": true,
		"Parents": [21],
		"Children": [23],
		"Created": "2019-05-10T23:14:20.255206458-04:00",
		"Updated": "2019-05-24T18:11:46.214310274-04:00"
	},
	"3c281c385e064ed1b6ab93a9456a04eb": {
		"Branch": "",
		"Note": "production work 2019-04-26 to 2019-05-10",
		"Log": [],
		"UUID": "3c281c385e064ed1b6ab93a9456a04eb",
		"VersionID": 21,
		"Locked": true,
		"Parents": [20],
		"Children": [22],
		"Created": "2019-04-26T22:50:41.986227371-04:00",
		"Updated": "2019-05-10T23:14:20.255207999-04:00"
	},
	"415836cb8bee4d23ab7c9dbf5c5bb9c9": {
		"Branch": "",
		"Note": "glial synapses removed 2019-04-03",
		"Log": [],
		"UUID": "415836cb8bee4d23ab7c9dbf5c5bb9c9",
		"VersionID": 19,
		"Locked": true,
		"Parents": [18],
		"Children": [20],
		"Created": "2019-03-30T10:15:28.806145249-04:00",
		"Updated": "2019-04-03T21:33:44.28763199-04:00"
	},
	"4c450cd6e38e485aa366a06b85478fc9": {
		"Branch": "",
		"Note": "production proofreading last week 2019; final fixes to synapses",
		"Log": [],
		"UUID": "4c450cd6e38e485aa366a06b85478fc9",
		"VersionID": 44,
		"Locked": true,
		"Parents": [43],
		"Children": [45],
		"Created": "2019-12-30T01:41:12.429758272-05:00",
		"Updated": "2020-01-01T10:19:30.560392631-05:00"
	},
	"524883b20d9e4b61bcc34c5243963357": {
		"Branch": "",
		"Note": "Shinya finished with body annotations for release",
		"Log": [],
		"UUID": "524883b20d9e4b61bcc34c5243963357",
		"VersionID": 48,
		"Locked": true,
		"Parents": [47],
		"Children": [49],
		"Created": "2020-01-15T21:04:05.26679759-05:00",
		"Updated": "2020-01-20T02:11:59.880024695-05:00"
	},
	"52a13328874c4bb7b15dc4280da26576": {
		"Branch": "",
		"Note": "v1.0: production proofreading from 2019-12-05 until 2019-12-20",
		"Log": [],
		"UUID": "52a13328874c4bb7b15dc4280da26576",
		"VersionID": 42,
		"Locked": true,
		"Parents": [41],
		"Children": [43],
		"Created": "2019-12-11T19:44:42.738005529-05:00",
		"Updated": "2019-12-23T04:23:21.861376639-05:00"
	},
	"54f76775738e44fbba476d1a87a9194a": {
		"Branch": "",
		"Note": "production from 2018-11-22 until emdata4 migration 2018-12-12",
		"Log": [],
		"UUID": "54f76775738e44fbba476d1a87a9194a",
		"VersionID": 8,
		"Locked": true,
		"Parents": [7],
		"Children": [9],
		"Created": "2018-11-22T00:06:13.582039951-05:00",
		"Updated": "2018-12-13T01:18:51.48891055-05:00"
	},
	"569625b31af845479fe4f13ff53f868e": {
		"Branch": "",
		"Note": "production from 2019-05-24 to 2019-06-27",
		"Log": [],
		"UUID": "569625b31af845479fe4f13ff53f868e",
		"VersionID": 23,
		"Locked": true,
		"Parents": [22],
		"Children": [24],
		"Created": "2019-05-24T18:11:46.21430794-04:00",
		"Updated": "2019-06-27T23:57:49.317425189-04:00"
	},
	"57e8d8a85bb64942baf3282cff169f16": {
		"Branch": "",
		"Note": "production proofreading from 2019-12-23 until 2019-12-30; repair to synapses in v1.0",
		"Log": [],
		"UUID": "57e8d8a85bb64942baf3282cff169f16",
		"VersionID": 43,
		"Locked": true,
		"Parents": [42],
		"Children": [44],
		"Created": "2019-12-23T04:23:21.861371554-05:00",
		"Updated": "2019-12-30T01:41:12.429765882-05:00"
	},
	"5c549c3fe8b546f995a680e757b2b2c9": {
		"Branch": "",
		"Note": "patched central frankenbody (108632992) supervoxel (5813064484)",
		"Log": [],
		"UUID": "5c549c3fe8b546f995a680e757b2b2c9",
		"VersionID": 13,
		"Locked": true,
		"Parents": [12],
		"Children": [14],
		"Created": "2019-02-27T19:49:45.191859849-05:00",
		"Updated": "2019-02-28T04:25:27.949803959-05:00"
	},
	"5d42b907299449d1ad5428210c72f00e": {
		"Branch": "",
		"Note": "Committed for Prod Copy Server 11-14-18",
		"Log": [],
		"UUID": "5d42b907299449d1ad5428210c72f00e",
		"VersionID": 6,
		"Locked": true,
		"Parents": [5],
		"Children": [7],
		"Created": "2018-11-11T10:31:13.331927323-05:00",
		"Updated": "2018-11-14T22:21:47.367903147-05:00"
	},
	"614fc0ebe9c24154a5dbc2892effdf9e": {
		"Branch": "",
		"Note": "proofreading from 2020-01-01 to 2020-01-06",
		"Log": [],
		"UUID": "614fc0ebe9c24154a5dbc2892effdf9e",
		"VersionID": 45,
		"Locked": true,
		"Parents": [44],
		"Children": [46],
		"Created": "2020-01-01T10:19:30.560386797-05:00",
		"Updated": "2020-01-06T23:50:05.899909151-05:00"
	},
	"7919332f0da34e659d6f28b6457ebefb": {
		"Branch": "",
		"Note": "2019-11-18 Masked out segmentation non-neuropil; full reload of synapses and synapses_labelsz",
		"Log": [],
		"UUID": "7919332f0da34e659d6f28b6457ebefb",
		"VersionID": 35,
		"Locked": true,
		"Parents": [34],
		"Children": [36],
		"Created": "2019-11-18T18:11:59.121870599-05:00",
		"Updated": "2019-11-19T05:58:04.028883136-05:00"
	},
	"845fd4873a3748bd9cc27fcc9a83d0a2": {
		"Branch": "",
		"Note": "Production proofreading 2019-11-20 after mask cleanup.",
		"Log": [],
		"UUID": "845fd4873a3748bd9cc27fcc9a83d0a2",
		"VersionID": 38,
		"Locked": true,
		"Parents": [37],
		"Children": [39],
		"Created": "2019-11-20T06:08:15.209580486-05:00",
		"Updated": "2019-11-20T22:18:58.367243868-05:00"
	},
	"881e9bf8eb3449dfaae20342df74e555": {
		"Branch": "",
		"Note": "production proofreading from 2019-12-05 after initial frankenbody split",
		"Log": [],
		"UUID": "881e9bf8eb3449dfaae20342df74e555",
		"VersionID": 41,
		"Locked": true,
		"Parents": [40],
		"Children": [42],
		"Created": "2019-12-05T23:01:03.766713942-05:00",
		"Updated": "2019-12-11T19:44:42.738010251-05:00"
	},
	"8e643237779541cb8033c32fc0bc03a6": {
		"Branch": "",
		"Note": "proofreading from 2020-01-20 to 2020-01-23; time of release",
		"Log": [],
		"UUID": "8e643237779541cb8033c32fc0bc03a6",
		"VersionID": 49,
		"Locked": true,
		"Parents": [48],
		"Children": [50],
		"Created": "2020-01-20T02:11:59.880017032-05:00",
		"Updated": "2020-01-23T18:14:02.880457648-05:00"
	},
	"8f6ee9fde1764b60b0e07e2ef164b1ff": {
		"Branch": "",
		"Note": "production on 2019-02-27, before frankenbody patch attempt",
		"Log": [],
		"UUID": "8f6ee9fde1764b60b0e07e2ef164b1ff",
		"VersionID": 12,
		"Locked": true,
		"Parents": [11],
		"Children": [13],
		"Created": "2019-02-27T03:54:40.306510902-05:00",
		"Updated": "2019-02-27T19:49:45.191861589-05:00"
	},
	"93208e2d71354ca9bfd1a76184d20aed": {
		"Branch": "",
		"Note": "Committed for Neuprint Update 11-01-18",
		"Log": [],
		"UUID": "93208e2d71354ca9bfd1a76184d20aed",
		"VersionID": 4,
		"Locked": true,
		"Parents": [3],
		"Children": [5],
		"Created": "2018-10-26T20:13:07.764158081-04:00",
		"Updated": "2018-11-01T00:00:42.580214548-04:00"
	},
	"948a39ce617d44bbac217dbac04cca0c": {
		"Branch": "",
		"Note": "Frankenbody supervoxels split, but low-res pyramids have not been updated.",
		"Log": [],
		"UUID": "948a39ce617d44bbac217dbac04cca0c",
		"VersionID": 14,
		"Locked": true,
		"Parents": [13],
		"Children": [15],
		"Created": "2019-02-28T04:25:27.94980053-05:00",
		"Updated": "2019-02-28T06:48:15.840303138-05:00"
	},
	"9847cd138b1a4051b5c0670bdccdcfa3": {
		"Branch": "",
		"Note": "committed 2018-10-21 before sync code fix",
		"Log": ["2018-11-22T00:00:11-05:00  node 9847 synapses reload"],
		"UUID": "9847cd138b1a4051b5c0670bdccdcfa3",
		"VersionID": 7,
		"Locked": true,
		"Parents": [6],
		"Children": [8],
		"Created": "2018-11-14T22:21:47.36790105-05:00",
		"Updated": "2018-11-22T00:06:13.582044136-05:00"
	},
	"a0dfa4f260244946b7abbab4dcd2cc86": {
		"Branch": "",
		"Note": "production work 2019-04-03 to 2019-04-26, committed for Google sync",
		"Log": [],
		"UUID": "a0dfa4f260244946b7abbab4dcd2cc86",
		"VersionID": 20,
		"Locked": true,
		"Parents": [19],
		"Children": [21],
		"Created": "2019-04-03T21:33:44.287629052-04:00",
		"Updated": "2019-04-26T22:50:41.986228963-04:00"
	},
	"a21aa92104bc4db89f72cb69b4c17890": {
		"Branch": "",
		"Note": "committed 2019-03-13 before updating Michal server",
		"Log": [],
		"UUID": "a21aa92104bc4db89f72cb69b4c17890",
		"VersionID": 15,
		"Locked": true,
		"Parents": [14],
		"Children": [16, 17],
		"Created": "2019-02-28T06:48:15.840297474-05:00",
		"Updated": "2019-03-18T14:01:51.196933576-04:00"
	},
	"a902f307f6da44c4a312c1a40e4a9498": {
		"Branch": "",
		"Note": "Cleaning up non-brain mask from node 79193 (2019-11-19)",
		"Log": [],
		"UUID": "a902f307f6da44c4a312c1a40e4a9498",
		"VersionID": 37,
		"Locked": true,
		"Parents": [36],
		"Children": [38],
		"Created": "2019-11-19T21:18:14.715596889-05:00",
		"Updated": "2019-11-20T06:08:15.209586418-05:00"
	},
	"b98b4829e305479ca7ac4b17194c425b": {
		"Branch": "",
		"Note": "committed 2019-03-29 night before updating synapses",
		"Log": [],
		"UUID": "b98b4829e305479ca7ac4b17194c425b",
		"VersionID": 16,
		"Locked": true,
		"Parents": [15],
		"Children": [18],
		"Created": "2019-03-13T20:08:02.083459769-04:00",
		"Updated": "2019-03-29T22:38:01.917424147-04:00"
	},
	"ba49b90c3bf742c9863ad9e8259cac7d": {
		"Branch": "",
		"Note": "production from 2019-10-05 to 2019-10-27",
		"Log": [],
		"UUID": "ba49b90c3bf742c9863ad9e8259cac7d",
		"VersionID": 31,
		"Locked": true,
		"Parents": [30],
		"Children": [32],
		"Created": "2019-10-05T08:40:49.65785839-04:00",
		"Updated": "2019-10-27T22:48:01.892271484-04:00"
	},
	"c3131aca11d24b12a37bf26714a99cbf": {
		"Branch": "",
		"Note": "Manual proofreading 2019-11-19",
		"Log": [],
		"UUID": "c3131aca11d24b12a37bf26714a99cbf",
		"VersionID": 36,
		"Locked": true,
		"Parents": [35],
		"Children": [37],
		"Created": "2019-11-19T05:58:04.028873561-05:00",
		"Updated": "2019-11-19T21:18:14.71560365-05:00"
	},
	"c765cee836464136b244ca8769b5890e": {
		"Branch": "",
		"Note": "synapses updated 2019-03-29",
		"Log": [],
		"UUID": "c765cee836464136b244ca8769b5890e",
		"VersionID": 18,
		"Locked": true,
		"Parents": [16],
		"Children": [19],
		"Created": "2019-03-29T22:38:01.917422851-04:00",
		"Updated": "2019-03-30T10:15:28.80614695-04:00"
	},
	"cd070d4b954743d19b181280742a922f": {
		"Branch": "lou test",
		"Note": "to test my MB neuron subtype generation",
		"Log": [],
		"UUID": "cd070d4b954743d19b181280742a922f",
		"VersionID": 17,
		"Locked": false,
		"Parents": [15],
		"Children": [],
		"Created": "2019-03-18T14:01:51.196930842-04:00",
		"Updated": "2019-03-18T14:01:51.196930842-04:00"
	},
	"d59e3fefce7b443ab476be77b8e7d15a": {
		"Branch": "",
		"Note": "Committed for Neuprint Update 11-10-18",
		"Log": [],
		"UUID": "d59e3fefce7b443ab476be77b8e7d15a",
		"VersionID": 5,
		"Locked": true,
		"Parents": [4],
		"Children": [6],
		"Created": "2018-11-01T00:00:42.580212569-04:00",
		"Updated": "2018-11-11T10:31:13.331929652-05:00"
	},
	"dd7b7e43bd0d4054aeb554633dfc4857": {
		"Branch": "",
		"Note": "production from 2019-09-29 to 2019-10-04 with synapse resync at end",
		"Log": [],
		"UUID": "dd7b7e43bd0d4054aeb554633dfc4857",
		"VersionID": 30,
		"Locked": true,
		"Parents": [29],
		"Children": [31],
		"Created": "2019-09-29T14:40:26.343515455-04:00",
		"Updated": "2019-10-05T08:40:49.657865435-04:00"
	},
	"e939fd903fa24c46b79984cef120b04a": {
		"Branch": "",
		"Note": "proofreading from 2020-01-07 to 2020-01-12; pre-flattened",
		"Log": [],
		"UUID": "e939fd903fa24c46b79984cef120b04a",
		"VersionID": 46,
		"Locked": true,
		"Parents": [45],
		"Children": [47],
		"Created": "2020-01-06T23:50:05.899906509-05:00",
		"Updated": "2020-01-12T00:58:31.500717842-05:00"
	},
	"ec21b712e508451eacb9a4a4f694f7d5": {
		"Branch": "",
		"Note": "Committed for Neuprint Update 102718",
		"Log": [],
		"UUID": "ec21b712e508451eacb9a4a4f694f7d5",
		"VersionID": 3,
		"Locked": true,
		"Parents": [2],
		"Children": [4],
		"Created": "2018-10-23T12:20:42.090065828-04:00",
		"Updated": "2018-10-26T20:13:07.76416016-04:00"
	},
	"ef1da1a01c4941b6876ab77502843d0c": {
		"Branch": "",
		"Note": "Applied Cleaves 102218, Manual Proofreading 102318 AM",
		"Log": [],
		"UUID": "ef1da1a01c4941b6876ab77502843d0c",
		"VersionID": 2,
		"Locked": true,
		"Parents": [1],
		"Children": [3],
		"Created": "2018-10-15T12:06:14.918689272-04:00",
		"Updated": "2018-10-23T12:20:42.090067709-04:00"
	},
	"f2cad8c5ff4341ddbb27ac4dcbe06034": {
		"Branch": "",
		"Note": "Production until 2019-11-16 before segmentation mask",
		"Log": [],
		"UUID": "f2cad8c5ff4341ddbb27ac4dcbe06034",
		"VersionID": 33,
		"Locked": true,
		"Parents": [32],
		"Children": [34],
		"Created": "2019-11-09T23:57:47.132203216-05:00",
		"Updated": "2019-11-18T02:00:47.870471583-05:00"
	},
	"f6565acc25a6405a8404191a09ee42b4": {
		"Branch": "",
		"Note": "Production proofreading from 2019-11-21 to 2019-12-05 before frankenbody cleanup",
		"Log": [],
		"UUID": "f6565acc25a6405a8404191a09ee42b4",
		"VersionID": 39,
		"Locked": true,
		"Parents": [38],
		"Children": [40],
		"Created": "2019-11-20T22:18:58.367241145-05:00",
		"Updated": "2019-12-05T20:30:08.567954245-05:00"
	},
	"f8f6759345f64fd396d9c89b2ab7fee2": {
		"Branch": "",
		"Note": "production from 2019-09-26 (after deletion of 2.4 million double PSDs)",
		"Log": [],
		"UUID": "f8f6759345f64fd396d9c89b2ab7fee2",
		"VersionID": 29,
		"Locked": true,
		"Parents": [28],
		"Children": [30],
		"Created": "2019-09-26T23:40:14.014926704-04:00",
		"Updated": "2019-09-29T14:40:26.343522568-04:00"
	},
	"fbfe6889f6d040babf5047b6cb0a3ca5": {
		"Branch": "",
		"Note": "proofreading from 2020-01-23 6 pm after all backports and hemibrain release",
		"Log": [],
		"UUID": "fbfe6889f6d040babf5047b6cb0a3ca5",
		"VersionID": 50,
		"Locked": false,
		"Parents": [49],
		"Children": [],
		"Created": "2020-01-23T18:14:02.880444095-05:00",
		"Updated": "2020-01-23T18:14:02.880444095-05:00"
	},
	"ff53bf786cc6423b9954f0e7c3d800e9": {
		"Branch": "",
		"Note": "production from 2018-12-23 until emdata4 migration 2018-12-16",
		"Log": [],
		"UUID": "ff53bf786cc6423b9954f0e7c3d800e9",
		"VersionID": 9,
		"Locked": true,
		"Parents": [8],
		"Children": [10],
		"Created": "2018-12-13T01:18:51.488903128-05:00",
		"Updated": "2018-12-16T17:00:53.556072019-05:00"
	}
}`
