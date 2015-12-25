package logkey

import "testing"

func TestKeys(t *testing.T) {
	ignored()
	if Filename.String() != "filename" {
		t.Error("unexpected func")
	}
	// Nothing to test.  Just to signal that we have coverage in this directory
}
