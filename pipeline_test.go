package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

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

func TestPipeline_Parallel_Success(t *testing.T) {
	size := 100
	data := make([]int, size)
	delay := time.Millisecond * 20
	executionEpselon := (delay * time.Duration(size)) / 2

	for i := range data {
		data[i] = i
	}

	testCases := []struct {
		name             string
		opts             []option
		minExecutionTime time.Duration
		maxExecutionTime time.Duration
	}{
		{
			name:             "no parallel",
			minExecutionTime: delay * time.Duration(size),
			maxExecutionTime: delay*time.Duration(size) + executionEpselon,
		},
		{
			name:             "5 parallel stages",
			opts:             []option{WithParallelStages(5)},
			minExecutionTime: (delay * time.Duration(size)) / time.Duration(5),
			maxExecutionTime: (delay*time.Duration(size))/time.Duration(100) + executionEpselon,
		},
		{
			name:             "100 parallel stages",
			opts:             []option{WithParallelStages(100)},
			minExecutionTime: (delay * time.Duration(size)) / time.Duration(100),
			maxExecutionTime: (delay*time.Duration(size))/time.Duration(100) + executionEpselon,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			p := newPipeline(data, testCase.opts...)
			p = withFilterStage("mod 25", p, func(ctx context.Context, v int) (bool, error) {
				time.Sleep(delay)
				return v%25 == 0, nil
			})
			p = withTransformerStage("mul 1", p, func(ctx context.Context, v int) (int, error) { return v * 1, nil })
			p = withFlatterStage("doubled", p, func(ctx context.Context, v int) ([]int, error) { return []int{v, v}, nil })
			ap := withAggregatorStage("even/odd", p, func(ctx context.Context, v int) (string, error) {
				if v%2 == 0 {
					return "even", nil
				}
				return "odd", nil
			})
			startTime := time.Now()
			aggregatedPairs, err := ap.eval(context.Background())
			duration := time.Since(startTime)
			require.NoError(t, err)
			expected := []AggregatedPair[string, int]{
				aggregateItem[string, int]{key: "even", value: []int{0, 0, 50, 50}},
				aggregateItem[string, int]{key: "odd", value: []int{25, 25, 75, 75}},
			}
			assert.Len(t, aggregatedPairs, len(expected))
			for _, ev := range expected {
				var av []int
				for _, v := range aggregatedPairs {
					if v.Key() == ev.Key() {
						av = v.Values()
						break
					}
				}
				assert.NotNil(t, av)
				assert.ElementsMatch(t, ev.Values(), av)
			}
			assert.NoError(t, ap.verifyClosedChannel())
			assert.GreaterOrEqual(t, duration, testCase.minExecutionTime)
			assert.LessOrEqual(t, duration, testCase.maxExecutionTime)
		})
	}
}

func TestPipeline_Parallel_ErrorOnStage(t *testing.T) {
	size := 100
	data := make([]int, size)
	retError := errors.New("error on 75 value")

	for i := range data {
		data[i] = i
	}

	testCases := []struct {
		name string
		opts []option
	}{
		{
			name: "no parallel",
		},
		{
			name: "5 parallel stages",
			opts: []option{WithParallelStages(5)},
		},
		{
			name: "100 parallel stages",
			opts: []option{WithParallelStages(100)},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			p := newPipeline(data, testCase.opts...)
			p = withFilterStage("mod 25", p, func(ctx context.Context, v int) (bool, error) {
				if v == 75 {
					return false, retError
				}
				return v%25 == 0, nil
			})
			p = withTransformerStage("mul 1", p, func(ctx context.Context, v int) (int, error) { return v * 1, nil })
			p = withFlatterStage("doubled", p, func(ctx context.Context, v int) ([]int, error) { return []int{v, v}, nil })
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
		})
	}
}
