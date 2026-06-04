package datastore

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/janelia-flyem/dvid/dvid"
)

type repoJSONForPublicTest struct {
	Root dvid.UUID
	Log  []string
	DAG  struct {
		Root  dvid.UUID
		Nodes map[dvid.UUID]struct {
			VersionID dvid.VersionID
			Parents   []dvid.VersionID
			Children  []dvid.VersionID
		}
	}
}

func mustCommit(t *testing.T, uuid dvid.UUID) {
	t.Helper()
	if err := Commit(uuid, "", nil); err != nil {
		t.Fatalf("could not commit %s: %v", uuid, err)
	}
}

func mustNewVersion(t *testing.T, parent dvid.UUID, branch string) dvid.UUID {
	t.Helper()
	child, err := NewVersion(parent, "", branch, nil)
	if err != nil {
		t.Fatalf("could not create version from %s on branch %q: %v", parent, branch, err)
	}
	return child
}

func mustVersion(t *testing.T, uuid dvid.UUID) dvid.VersionID {
	t.Helper()
	v, err := VersionFromUUID(uuid)
	if err != nil {
		t.Fatalf("could not resolve version for %s: %v", uuid, err)
	}
	return v
}

func decodeRepoJSONForPublicTest(t *testing.T, jsonStr string) repoJSONForPublicTest {
	t.Helper()
	var repo repoJSONForPublicTest
	if err := json.Unmarshal([]byte(jsonStr), &repo); err != nil {
		t.Fatalf("could not decode repo JSON: %v\n%s", err, jsonStr)
	}
	return repo
}

func TestSetPublicVersionsEmptyWithoutManager(t *testing.T) {
	Shutdown()
	if err := SetPublicVersions(nil); err != nil {
		t.Fatalf("empty public_versions should not require initialized manager: %v", err)
	}
	if IsPublic(dvid.UUID("0123456789abcdef0123456789abcdef")) {
		t.Fatalf("empty public_versions should not mark any UUID public")
	}
}

func TestSetPublicVersionsValidationAndClosure(t *testing.T) {
	OpenTest()
	defer CloseTest()

	root, _ := NewTestRepo()
	if err := SetPublicVersions([]string{string(root)}); err == nil {
		t.Fatalf("expected unlocked root to be rejected as public release")
	}
	mustCommit(t, root)

	left := mustNewVersion(t, root, "")
	mustCommit(t, left)
	right := mustNewVersion(t, root, "right")
	mustCommit(t, right)
	merge, err := Merge([]dvid.UUID{left, right}, "merge", MergeConflictFree)
	if err != nil {
		t.Fatalf("could not merge public parents: %v", err)
	}
	mustCommit(t, merge)
	private := mustNewVersion(t, merge, "")

	if err := SetPublicVersions([]string{string(merge)}); err != nil {
		t.Fatalf("could not configure merge release as public: %v", err)
	}
	for _, uuid := range []dvid.UUID{root, left, right, merge} {
		if !IsPublic(uuid) {
			t.Fatalf("expected %s to be public", uuid)
		}
		if !IsPublicVersion(mustVersion(t, uuid)) {
			t.Fatalf("expected version for %s to be public", uuid)
		}
	}
	if IsPublic(private) {
		t.Fatalf("private descendant %s should not be public", private)
	}

	for _, bad := range []string{
		":master",
		string(root) + ":master",
		string(root) + ":master~1",
		string(root[:5]),
		"0123456789abcdef0123456789abcdef",
		string(private),
	} {
		if err := SetPublicVersions([]string{bad}); err == nil {
			t.Fatalf("expected public_versions entry %q to be rejected", bad)
		}
	}
	if !IsPublic(merge) || IsPublic(private) {
		t.Fatalf("failed SetPublicVersions call should not replace prior public set")
	}
}

func TestRepoJSONFilteredPublicDAG(t *testing.T) {
	OpenTest()
	defer CloseTest()

	root, _ := NewTestRepo()
	mustCommit(t, root)
	child := mustNewVersion(t, root, "")
	mustCommit(t, child)
	private := mustNewVersion(t, child, "")
	privateV := mustVersion(t, private)
	if err := AddToRepoLog(root, []string{"private uuid " + string(private)}); err != nil {
		t.Fatalf("could not add repo log: %v", err)
	}
	hiddenRoot, _ := NewTestRepo()

	if err := SetPublicVersions([]string{string(child)}); err != nil {
		t.Fatalf("could not configure public child: %v", err)
	}
	jsonStr, err := GetRepoJSONFiltered(root, RepoPublicOnly)
	if err != nil {
		t.Fatalf("could not get filtered repo JSON: %v", err)
	}
	repo := decodeRepoJSONForPublicTest(t, jsonStr)
	if repo.Root != root || repo.DAG.Root != root {
		t.Fatalf("filtered repo root changed: repo %s, dag %s, expected %s", repo.Root, repo.DAG.Root, root)
	}
	if len(repo.Log) != 0 {
		t.Fatalf("public repo JSON should omit repo log, got %v", repo.Log)
	}
	if _, found := repo.DAG.Nodes[private]; found {
		t.Fatalf("filtered DAG leaked private node %s", private)
	}
	if len(repo.DAG.Nodes) != 2 {
		t.Fatalf("expected root and child only, got %d nodes: %v", len(repo.DAG.Nodes), repo.DAG.Nodes)
	}
	versionIDs := make(map[dvid.VersionID]struct{}, len(repo.DAG.Nodes))
	for _, node := range repo.DAG.Nodes {
		versionIDs[node.VersionID] = struct{}{}
		if node.VersionID == privateV {
			t.Fatalf("filtered DAG leaked private version %d", privateV)
		}
	}
	for uuid, node := range repo.DAG.Nodes {
		for _, childV := range node.Children {
			if _, found := versionIDs[childV]; !found {
				t.Fatalf("filtered DAG node %s has dangling/private child version %d", uuid, childV)
			}
		}
	}

	jsonBytes, err := MarshalJSONFiltered(func(rootUUID dvid.UUID) RepoVisibility {
		return RepoPublicOnly
	})
	if err != nil {
		t.Fatalf("could not marshal filtered repos: %v", err)
	}
	var repos map[dvid.UUID]repoJSONForPublicTest
	if err := json.Unmarshal(jsonBytes, &repos); err != nil {
		t.Fatalf("could not decode filtered repos JSON: %v\n%s", err, string(jsonBytes))
	}
	if _, found := repos[root]; !found {
		t.Fatalf("filtered repos missing public repo %s: %s", root, string(jsonBytes))
	}
	if _, found := repos[hiddenRoot]; found {
		t.Fatalf("filtered repos should hide zero-public repo %s: %s", hiddenRoot, string(jsonBytes))
	}
}

func TestBranchVersionsJSONFilteredPublicSuffix(t *testing.T) {
	OpenTest()
	defer CloseTest()

	root, _ := NewTestRepo()
	mustCommit(t, root)
	publicHead := mustNewVersion(t, root, "")
	mustCommit(t, publicHead)
	privateHead := mustNewVersion(t, publicHead, "")
	mustCommit(t, privateHead)

	if err := SetPublicVersions([]string{string(publicHead)}); err != nil {
		t.Fatalf("could not configure public head: %v", err)
	}
	jsonStr, err := GetBranchVersionsJSONFiltered(root, "master", RepoPublicOnly)
	if err != nil {
		t.Fatalf("could not get filtered branch versions: %v", err)
	}
	var got []dvid.UUID
	if err := json.Unmarshal([]byte(jsonStr), &got); err != nil {
		t.Fatalf("could not decode filtered branch versions: %v\n%s", err, jsonStr)
	}
	expected := []dvid.UUID{publicHead, root}
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("expected public suffix %v, got %v", expected, got)
	}
	if IsPublic(privateHead) {
		t.Fatalf("private branch head %s should not be public", privateHead)
	}
}
