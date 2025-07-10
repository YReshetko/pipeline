package pipeline

import "testing"

func TestBaseStageInit(t *testing.T) {
	s := baseStage[bool, int]{}
	s.init()
}
