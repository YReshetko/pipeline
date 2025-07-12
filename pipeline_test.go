package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPipeline_Success(t *testing.T) {
	p := newPipeline([]int{1, 2, 3, 4, 5})
	p = withFilterStage("remove 3", p, func(ctx context.Context, v int) (bool, error) { return v != 3, nil })
	p = withTransformerStage("mul 3", p, func(ctx context.Context, v int) (int, error) { return v * 3, nil })
	p = withFlatterStage("doubled", p, func(ctx context.Context, v int) ([]int, error) { return []int{v, v}, nil })
	ap := withAggregatorStage("even/odd", p, func(ctx context.Context, v int) (string, error) {
		if v%2 == 0 {
			return "even", nil
		}
		return "odd", nil
	})
	aggregatedPairs, err := ap.eval(context.Background())
	require.NoError(t, err)
	assert.ElementsMatch(t, []AggregatedPair[string, int]{
		aggregateItem[string, int]{key: "even", value: []int{6, 6, 12, 12}},
		aggregateItem[string, int]{key: "odd", value: []int{3, 3, 15, 15}},
	}, aggregatedPairs)
	assert.NoError(t, ap.verifyClosedChannel())
}

func TestPipeline_ErrorOnStage(t *testing.T) {
	retError := errors.New("error on double stage")
	p := newPipeline([]int{1, 2, 3, 4, 5})
	p = withFilterStage("remove 3", p, func(ctx context.Context, v int) (bool, error) { return v != 3, nil })
	p = withTransformerStage("mul 3", p, func(ctx context.Context, v int) (int, error) { return v * 3, nil })
	p = withFlatterStage("doubled", p, func(ctx context.Context, v int) ([]int, error) { return []int{v, v}, retError })
	ap := withAggregatorStage("even/odd", p, func(ctx context.Context, v int) (string, error) {
		if v%2 == 0 {
			return "even", nil
		}
		return "odd", nil
	})
	aggregatedPairs, err := ap.eval(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, retError)
	assert.Empty(t, aggregatedPairs)
	assert.NoError(t, ap.verifyClosedChannel())
}

func TestPipeline_PanicOnStage(t *testing.T) {
	retError := errors.New("panic on even/odd stage")
	p := newPipeline([]int{1, 2, 3, 4, 5})
	p = withFilterStage("remove 3", p, func(ctx context.Context, v int) (bool, error) { return v != 3, nil })
	p = withTransformerStage("mul 3", p, func(ctx context.Context, v int) (int, error) { return v * 3, nil })
	p = withFlatterStage("doubled", p, func(ctx context.Context, v int) ([]int, error) { return []int{v, v}, nil })
	ap := withAggregatorStage("even/odd", p, func(ctx context.Context, v int) (string, error) {
		if v%2 == 0 {
			panic(retError)
		}
		return "odd", nil
	})
	aggregatedPairs, err := ap.eval(context.Background())
	require.Error(t, err)
	assert.ErrorContains(t, err, retError.Error())
	assert.Empty(t, aggregatedPairs)
	assert.NoError(t, ap.verifyClosedChannel())
}

func TestPipeline_ContextClosed(t *testing.T) {
	ctx, fn := context.WithCancel(context.Background())
	fn()
	p := newPipeline([]int{1, 2, 3, 4, 5})
	p = withFilterStage("remove 3", p, func(ctx context.Context, v int) (bool, error) { return v != 3, nil })
	p = withTransformerStage("mul 3", p, func(ctx context.Context, v int) (int, error) { return v * 3, nil })
	p = withFlatterStage("doubled", p, func(ctx context.Context, v int) ([]int, error) { return []int{v, v}, nil })
	ap := withAggregatorStage("even/odd", p, func(ctx context.Context, v int) (string, error) {
		if v%2 == 0 {
			return "even", nil
		}
		return "odd", nil
	})
	aggregatedPairs, err := ap.eval(ctx)
	require.NoError(t, err)
	assert.Empty(t, aggregatedPairs)
	assert.NoError(t, ap.verifyClosedChannel())
}

func TestPipeline_ContextClosedDuringInvocations(t *testing.T) {
	ctx, fn := context.WithCancel(context.Background())
	p := newPipeline([]int{1, 2, 3, 4, 5})
	p = withFilterStage("remove 3", p, func(ctx context.Context, v int) (bool, error) { return v != 3, nil })
	p = withTransformerStage("mul 3", p, func(ctx context.Context, v int) (int, error) { return v * 3, nil })
	p = withFlatterStage("doubled", p, func(ctx context.Context, v int) ([]int, error) { return []int{v, v}, nil })
	ap := withAggregatorStage("even/odd", p, func(ctx context.Context, v int) (string, error) {
		if v%2 == 0 {
			fn()
			return "even", nil
		}
		return "odd", nil
	})
	aggregatedPairs, err := ap.eval(ctx)
	require.NoError(t, err)
	assert.Empty(t, aggregatedPairs)
	assert.NoError(t, ap.verifyClosedChannel())
}

func TestPipeline_PanicOnStage_NoRecovery(t *testing.T) {
	// Verfies that noRecovery option warks, can be run manually
	t.Skip()
	retError := errors.New("panic on even/odd stage")
	p := newPipeline([]int{1, 2, 3, 4, 5}, WithNoRecoveryOption())
	p = withFilterStage("remove 3", p, func(ctx context.Context, v int) (bool, error) { return v != 3, nil })
	p = withTransformerStage("mul 3", p, func(ctx context.Context, v int) (int, error) { return v * 3, nil })
	p = withFlatterStage("doubled", p, func(ctx context.Context, v int) ([]int, error) { return []int{v, v}, nil })
	ap := withAggregatorStage("even/odd", p, func(ctx context.Context, v int) (string, error) {
		if v%2 == 0 {
			panic(retError)
		}
		return "odd", nil
	})
	assert.Panics(t, func() { ap.eval(context.Background()) })

}
