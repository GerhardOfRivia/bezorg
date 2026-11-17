package transfer

import "testing"

func TestPlanLocal(t *testing.T) {
	cases := []struct {
		name                string
		finalExists         bool
		finalSize           int64
		partialExists       bool
		partialSize, remote int64
		want                Action
		offset              int64
	}{
		{"fresh", false, 0, false, 0, 10, ActionFetch, 0},
		{"skip match", true, 10, false, 0, 10, ActionSkip, 0},
		{"replace mismatch", true, 3, false, 0, 10, ActionFetch, 0},
		{"resume", false, 0, true, 4, 10, ActionFetch, 4},
		{"finish partial", false, 0, true, 10, 10, ActionFinish, 0},
		{"partial too long", false, 0, true, 12, 10, ActionFetch, 0},
		{"unknown resume", false, 0, true, 4, -1, ActionFetch, 4},
		{"unknown skip", true, 4, false, 0, -1, ActionSkip, 0},
		{"empty skip", true, 0, false, 0, 0, ActionSkip, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := PlanLocal(tc.finalExists, tc.finalSize, tc.partialExists, tc.partialSize, tc.remote)
			if got.Action != tc.want || got.Offset != tc.offset {
				t.Fatalf("plan = %+v, want action %v offset %d", got, tc.want, tc.offset)
			}
		})
	}
}

func TestIsLarge(t *testing.T) {
	min := int64(64 << 20)
	if IsLarge("note.xml", 100, min) {
		t.Fatal("small xml should stay on the small lane")
	}
	if !IsLarge("frame.tif", 80<<20, min) {
		t.Fatal("large tif should use the large lane")
	}
	if IsLarge("preview.tif", 1<<20, min) {
		t.Fatal("tiny preview tif should stay on the small lane")
	}
	if !IsLarge("scene.ntf", -1, min) {
		t.Fatal("unknown ntf should use the large lane")
	}
	if !IsLarge("blob.bin", min, min) {
		t.Fatal("threshold-sized file should use the large lane")
	}
}

func TestCleanRel(t *testing.T) {
	got, err := CleanRel(`sub\dir\file.tif`)
	if err != nil || got != "sub/dir/file.tif" {
		t.Fatalf("clean = %q %v", got, err)
	}
	if _, err := CleanRel("../secret"); err == nil {
		t.Fatal("expected escape to fail")
	}
}
