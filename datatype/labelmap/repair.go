package labelmap

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
	pb "google.golang.org/protobuf/proto"
)

// maxMappedLabel returns the maximum supervoxel or body label present in the
// mapping visible at the given version.  Split-created supervoxels appear
// here even when they exceed every body (index) label.
func (vc *VCache) maxMappedLabel(v dvid.VersionID) (maxLabel uint64) {
	mappedVersions := vc.getMappedVersionsDist(v)
	for _, lmap := range vc.mapShards {
		lmap.fmMu.RLock()
		for fromLabel, vm := range lmap.fm {
			toLabel, present := vm.value(mappedVersions)
			if present {
				if fromLabel > maxLabel {
					maxLabel = fromLabel
				}
				if toLabel > maxLabel {
					maxLabel = toLabel
				}
			}
		}
		lmap.fmMu.RUnlock()
	}
	return
}

// maxLabelInIndexes returns the maximum body label and supervoxel ID over all
// label indices visible at the given version.
func (d *Data) maxLabelInIndexes(v dvid.VersionID) (maxLabel uint64, numIndices uint64, err error) {
	var store storage.OrderedKeyValueDB
	store, err = datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return
	}
	ctx := datastore.NewVersionedCtx(d, v)
	begTKey := NewLabelIndexTKey(0)
	endTKey := NewLabelIndexTKey(math.MaxUint64)
	err = store.ProcessRange(ctx, begTKey, endTKey, nil, func(c *storage.Chunk) error {
		if c == nil || c.V == nil || len(c.V) == 0 {
			return nil
		}
		label, err := DecodeLabelIndexTKey(c.K)
		if err != nil {
			return err
		}
		numIndices++
		if label > maxLabel {
			maxLabel = label
		}
		data, _, err := dvid.DeserializeData(c.V, true)
		if err != nil {
			return fmt.Errorf("unable to deserialize label index %d: %v", label, err)
		}
		idx := new(labels.Index)
		if err := pb.Unmarshal(data, idx); err != nil {
			return fmt.Errorf("unable to unmarshal label index %d: %v", label, err)
		}
		for _, svc := range idx.Blocks {
			if svc != nil {
				for sv := range svc.Counts {
					if sv > maxLabel {
						maxLabel = sv
					}
				}
			}
		}
		return nil
	})
	return
}

// repoVersions returns all version IDs in the repo DAG holding the given UUID.
func repoVersions(uuid dvid.UUID) ([]dvid.VersionID, error) {
	rootUUID, err := datastore.GetRepoRoot(uuid)
	if err != nil {
		return nil, err
	}
	rootV, err := datastore.VersionFromUUID(rootUUID)
	if err != nil {
		return nil, err
	}
	var versions []dvid.VersionID
	seen := make(map[dvid.VersionID]struct{})
	stack := []dvid.VersionID{rootV}
	for len(stack) > 0 {
		v := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if _, found := seen[v]; found {
			continue
		}
		seen[v] = struct{}{}
		versions = append(versions, v)
		children, err := datastore.GetChildrenByVersion(v)
		if err != nil {
			return nil, err
		}
		stack = append(stack, children...)
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })
	return versions, nil
}

// repairMaxLabels recomputes per-version max labels and the repo-wide max
// label from ground truth -- label indices plus supervoxel->body mappings,
// so split-created supervoxels are counted -- and reports stored versus
// recomputed values.  With commit=false (the default) nothing is written.
// With commit=true the recomputed values are persisted; this is the only
// code path allowed to lower MaxRepoLabel, and only to the verified
// recomputed value.
func (d *Data) repairMaxLabels(uuid dvid.UUID, commit bool) (string, error) {
	versions, err := repoVersions(uuid)
	if err != nil {
		return "", err
	}

	// Ground truth is computed before taking the counter lock since it
	// involves potentially slow store scans.
	recomputed := make(map[dvid.VersionID]uint64, len(versions))
	var repoRecomputed uint64
	for _, v := range versions {
		idxMax, numIndices, err := d.maxLabelInIndexes(v)
		if err != nil {
			return "", fmt.Errorf("recomputing max label for version %d: %v", v, err)
		}
		vc, err := getMapping(d, v)
		if err != nil {
			return "", fmt.Errorf("getting mappings for version %d: %v", v, err)
		}
		gt := idxMax
		if mapMax := vc.maxMappedLabel(v); mapMax > gt {
			gt = mapMax
		}
		recomputed[v] = gt
		if gt > repoRecomputed {
			repoRecomputed = gt
		}
		dvid.Infof("repair-maxlabel data %q version %d: %d indices scanned, recomputed max %d\n",
			d.DataName(), v, numIndices, gt)
	}

	mode := "dry-run"
	if commit {
		mode = "commit"
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "repair-maxlabel (%s) for data %q:\n", mode, d.DataName())

	d.mlMu.Lock()
	defer d.mlMu.Unlock()

	for _, v := range versions {
		versionUUID, err := datastore.UUIDFromVersion(v)
		if err != nil {
			return "", err
		}
		gt := recomputed[v]
		stored, found := d.MaxLabel[v]
		storedStr := "none"
		if found {
			storedStr = fmt.Sprintf("%d", stored)
		}
		switch {
		case gt == 0:
			if found {
				fmt.Fprintf(&sb, "  version %s: stored max %s, no ground truth found -- left unchanged\n", versionUUID, storedStr)
			}
		case found && stored == gt:
			fmt.Fprintf(&sb, "  version %s: stored max %s matches recomputed value\n", versionUUID, storedStr)
		default:
			action := "would set to"
			if commit {
				d.MaxLabel[v] = gt
				if err := d.persistMaxLabel(v); err != nil {
					return sb.String(), fmt.Errorf("persisting max label for version %s: %v", versionUUID, err)
				}
				action = "set to"
			}
			fmt.Fprintf(&sb, "  version %s: stored max %s, %s recomputed %d\n", versionUUID, storedStr, action, gt)
		}
	}

	storedRepo := d.MaxRepoLabel
	switch {
	case repoRecomputed == 0:
		fmt.Fprintf(&sb, "  repo-wide max: stored %d, no ground truth found -- refusing to change\n", storedRepo)
	case storedRepo == repoRecomputed:
		fmt.Fprintf(&sb, "  repo-wide max: stored %d matches recomputed value\n", storedRepo)
	default:
		action := "would set to"
		if commit {
			d.MaxRepoLabel = repoRecomputed
			if err := d.persistMaxRepoLabel(); err != nil {
				return sb.String(), fmt.Errorf("persisting repo-wide max label: %v", err)
			}
			action = "set to"
			dvid.Criticalf("repair-maxlabel --commit changed repo-wide max label of data %q from %d to %d\n",
				d.DataName(), storedRepo, repoRecomputed)
		}
		fmt.Fprintf(&sb, "  repo-wide max: stored %d, %s recomputed %d\n", storedRepo, action, repoRecomputed)
	}
	if d.NextLabel != 0 {
		fmt.Fprintf(&sb, "  next label override is active: %d (untouched by repair)\n", d.NextLabel)
	}
	sb.WriteString("NOTE: labels reserved via POST /nextlabel but not yet written to blocks or\n" +
		"mappings cannot be detected; run during proofreading quiescence before committing.\n")
	if !commit {
		sb.WriteString("(dry run -- rerun with --commit to persist recomputed values)\n")
	}
	return sb.String(), nil
}
