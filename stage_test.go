package pipeline

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBaseStage_Success(t *testing.T) {
	s, sc := newBaseStage(func(ctx context.Context, i int) (string, error) {
		return strconv.Itoa(i), nil
	})
	s.init()
	res, err := runData(s, sc, noErr, 1, 2, 3)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2", "3"}, res)
	assert.NoError(t, s.verifyClosedChannel())
}

func TestBaseStage_ErrorOnStage(t *testing.T) {
	retErr := errors.New("error on stage")
	s, sc := newBaseStage(func(ctx context.Context, i int) (string, error) {
		if i == 2 {
			return "", retErr
		}
		return strconv.Itoa(i), nil
	})
	s.init()
	res, err := runData(s, sc, noErr, 1, 2, 3)
	require.Error(t, err)
	assert.ErrorIs(t, err, retErr)
	assert.Equal(t, []string{"1"}, res)
	assert.NoError(t, s.verifyClosedChannel())
}

func TestBaseStage_PanicOnStage(t *testing.T) {
	retErr := errors.New("panic on stage")
	s, sc := newBaseStage(func(ctx context.Context, i int) (string, error) {
		if i == 2 {
			panic(retErr)
		}
		return strconv.Itoa(i), nil
	})
	s.init()
	res, err := runData(s, sc, noErr, 1, 2, 3)
	require.Error(t, err)
	assert.ErrorContains(t, err, retErr.Error())
	assert.Equal(t, []string{"1"}, res)
	assert.NoError(t, s.verifyClosedChannel())
}

func TestBaseStage_ErrorOnPrevStage(t *testing.T) {
	retErr := errors.New("error on prev stage")
	s, sc := newBaseStage(func(ctx context.Context, i int) (string, error) {
		return strconv.Itoa(i), nil
	})
	s.init()
	res, err := runData(s, sc, func(v int) error {
		if v == 2 {
			return retErr
		}
		return nil
	}, 1, 2, 3)
	require.Error(t, err)
	assert.ErrorIs(t, err, retErr)
	assert.Equal(t, []string{"1"}, res)
	assert.NoError(t, s.verifyClosedChannel())
}

func TestBaseStage_NextStageIsClosed(t *testing.T) {
	s, sc := newBaseStage(func(ctx context.Context, i int) (string, error) {
		return strconv.Itoa(i), nil
	})
	s.init()
	s.cancelFn()
	res, err := runData(s, sc, noErr, 1, 2, 3)
	require.NoError(t, err)
	assert.Equal(t, []string{}, res)
	assert.NoError(t, s.verifyClosedChannel())
}

func TestTransformerStage_Success(t *testing.T) {
	s, sc := newBaseStage(func(ctx context.Context, i int) (string, error) {
		return strconv.Itoa(i), nil
	})
	ts := transformerStage[int, string]{baseStage: s}
	ts.init()
	res, err := runData(ts.baseStage, sc, noErr, 1, 2, 3)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2", "3"}, res)
	assert.NoError(t, ts.verifyClosedChannel())
}

func TestTransformerStage_ErrorOnStage(t *testing.T) {
	retErr := errors.New("error on stage")
	s, sc := newBaseStage(func(ctx context.Context, i int) (string, error) {
		if i == 2 {
			return "", retErr
		}
		return strconv.Itoa(i), nil
	})
	ts := transformerStage[int, string]{baseStage: s}
	ts.init()
	res, err := runData(ts.baseStage, sc, noErr, 1, 2, 3)
	require.Error(t, err)
	assert.ErrorIs(t, err, retErr)
	assert.Equal(t, []string{"1"}, res)
	assert.NoError(t, ts.verifyClosedChannel())
}

func TestTransformerStage_PanicOnStage(t *testing.T) {
	retErr := errors.New("panic on stage")
	s, sc := newBaseStage(func(ctx context.Context, i int) (string, error) {
		if i == 2 {
			panic(retErr)
		}
		return strconv.Itoa(i), nil
	})
	ts := transformerStage[int, string]{baseStage: s}
	ts.init()
	res, err := runData(ts.baseStage, sc, noErr, 1, 2, 3)
	require.Error(t, err)
	assert.ErrorContains(t, err, retErr.Error())
	assert.Equal(t, []string{"1"}, res)
	assert.NoError(t, ts.verifyClosedChannel())
}

func TestTransformerStage_ErrorOnPrevStage(t *testing.T) {
	retErr := errors.New("error on prev stage")
	s, sc := newBaseStage(func(ctx context.Context, i int) (string, error) {
		return strconv.Itoa(i), nil
	})
	ts := transformerStage[int, string]{baseStage: s}
	ts.init()
	res, err := runData(ts.baseStage, sc, func(v int) error {
		if v == 2 {
			return retErr
		}
		return nil
	}, 1, 2, 3)
	require.Error(t, err)
	assert.ErrorIs(t, err, retErr)
	assert.Equal(t, []string{"1"}, res)
	assert.NoError(t, ts.verifyClosedChannel())
}

func TestTransformerStage_NextStageIsClosed(t *testing.T) {
	s, sc := newBaseStage(func(ctx context.Context, i int) (string, error) {
		return strconv.Itoa(i), nil
	})
	ts := transformerStage[int, string]{baseStage: s}
	ts.init()
	ts.cancelFn()
	res, err := runData(ts.baseStage, sc, noErr, 1, 2, 3)
	require.NoError(t, err)
	assert.Equal(t, []string{}, res)
	assert.NoError(t, ts.verifyClosedChannel())
}

func TestEmitterStage_Success(t *testing.T) {
	s, sc := newBaseStage(func(ctx context.Context, i struct{}) (int, error) {
		return 0, nil
	})
	// The idea, emmiter stage does not contain in err channel and doesnt start listenong errors at all
	close(sc.inErr)
	es := emitterStage[int]{
		baseStage: s,
		data:      []int{1, 2, 3},
	}
	es.init()
	go es.run()

	res := make([]int, 0, 3)
	for v := range sc.out {
		res = append(res, v)
	}

	assert.Equal(t, []int{1, 2, 3}, res)
	assert.NoError(t, es.verifyClosedChannel())
}

func TestEmitterStage_NextStageIsClosed(t *testing.T) {
	s, sc := newBaseStage(func(ctx context.Context, i struct{}) (int, error) {
		return 0, nil
	})
	// The idea, emmiter stage does not contain in err channel and doesnt start listenong errors at all
	close(sc.inErr)
	es := emitterStage[int]{
		baseStage: s,
		data:      []int{1, 2, 3},
	}
	es.init()
	es.cancelFn()
	go es.run()

	res := make([]int, 0, 3)
	for v := range sc.out {
		res = append(res, v)
	}

	assert.NotNil(t, res)
	assert.NoError(t, es.verifyClosedChannel())
}

func TestFilterStage_Success(t *testing.T) {
	s, sc := newBaseStage(func(ctx context.Context, i int) (int, error) { return 0, nil })
	fs := filterStage[int]{
		baseStage: s,
		filter: func(ctx context.Context, i int) (bool, error) {
			return i%2 == 0, nil
		},
	}
	fs.init()
	res, err := runData(fs.baseStage, sc, noErr, 1, 2, 3, 4)
	require.NoError(t, err)
	assert.Equal(t, []int{2, 4}, res)
	assert.NoError(t, fs.verifyClosedChannel())
}

func TestFilterStage_ErrorOnStage(t *testing.T) {
	retErr := errors.New("error on stage")
	s, sc := newBaseStage(func(ctx context.Context, i int) (int, error) { return 0, nil })
	fs := filterStage[int]{
		baseStage: s,
		filter: func(ctx context.Context, i int) (bool, error) {
			if i == 4 {
				return false, retErr
			}
			return i%2 == 0, nil
		},
	}
	fs.init()
	res, err := runData(fs.baseStage, sc, noErr, 1, 2, 3, 4)
	require.Error(t, err)
	assert.ErrorIs(t, err, retErr)
	assert.Equal(t, []int{2}, res)
	assert.NoError(t, fs.verifyClosedChannel())
}

func TestFilterStage_PanicOnStage(t *testing.T) {
	retErr := errors.New("panic on stage")
	s, sc := newBaseStage(func(ctx context.Context, i int) (int, error) { return 0, nil })
	fs := filterStage[int]{
		baseStage: s,
		filter: func(ctx context.Context, i int) (bool, error) {
			if i == 4 {
				panic(retErr)
			}
			return i%2 == 0, nil
		},
	}
	fs.init()
	res, err := runData(fs.baseStage, sc, noErr, 1, 2, 3, 4)
	require.Error(t, err)
	assert.ErrorContains(t, err, retErr.Error())
	assert.Equal(t, []int{2}, res)
	assert.NoError(t, fs.verifyClosedChannel())
}

func TestFilterStage_ErrorOnPrevStage(t *testing.T) {
	retErr := errors.New("error on prev stage")
	s, sc := newBaseStage(func(ctx context.Context, i int) (int, error) { return 0, nil })
	fs := filterStage[int]{
		baseStage: s,
		filter: func(ctx context.Context, i int) (bool, error) {
			return i%2 == 0, nil
		},
	}
	fs.init()
	res, err := runData(fs.baseStage, sc, func(v int) error {
		if v == 4 {
			return retErr
		}
		return nil
	}, 1, 2, 3, 4)
	require.Error(t, err)
	assert.ErrorIs(t, err, retErr)
	assert.Equal(t, []int{2}, res)
	assert.NoError(t, fs.verifyClosedChannel())
}

func TestFilterStage_NextStageIsClosed(t *testing.T) {
	s, sc := newBaseStage(func(ctx context.Context, i int) (int, error) { return 0, nil })
	fs := filterStage[int]{
		baseStage: s,
		filter: func(ctx context.Context, i int) (bool, error) {
			return i%2 == 0, nil
		},
	}
	fs.init()
	fs.cancelFn()
	res, err := runData(fs.baseStage, sc, noErr, 1, 2, 3)
	require.NoError(t, err)
	assert.Equal(t, []int{}, res)
	assert.NoError(t, fs.verifyClosedChannel())
}

func TestFlatterStage_Success(t *testing.T) {
	s, sc := newBaseStage(func(ctx context.Context, i int) (string, error) { return "", nil })
	fs := flatterStage[int, string]{
		baseStage: s,
		fn: func(ctx context.Context, i int) ([]string, error) {
			return []string{strconv.Itoa(i), strconv.Itoa(i)}, nil
		},
	}
	fs.init()
	res, err := runData(fs.baseStage, sc, noErr, 1, 2, 3)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "1", "2", "2", "3", "3"}, res)
	assert.NoError(t, fs.verifyClosedChannel())
}

func TestFlatterStage_ErrorOnStage(t *testing.T) {
	retErr := errors.New("error on stage")
	s, sc := newBaseStage(func(ctx context.Context, i int) (string, error) { return "", nil })
	fs := flatterStage[int, string]{
		baseStage: s,
		fn: func(ctx context.Context, i int) ([]string, error) {
			if i == 2 {
				return nil, retErr
			}
			return []string{strconv.Itoa(i), strconv.Itoa(i)}, nil
		},
	}
	fs.init()
	res, err := runData(fs.baseStage, sc, noErr, 1, 2, 3)
	require.Error(t, err)
	assert.ErrorIs(t, err, retErr)
	assert.Equal(t, []string{"1", "1"}, res)
	assert.NoError(t, fs.verifyClosedChannel())
}

func TestFlatterStage_PanicOnStage(t *testing.T) {
	retErr := errors.New("panic on stage")
	s, sc := newBaseStage(func(ctx context.Context, i int) (string, error) { return "", nil })
	fs := flatterStage[int, string]{
		baseStage: s,
		fn: func(ctx context.Context, i int) ([]string, error) {
			if i == 2 {
				panic(retErr)
			}
			return []string{strconv.Itoa(i), strconv.Itoa(i)}, nil
		},
	}
	fs.init()
	res, err := runData(fs.baseStage, sc, noErr, 1, 2, 3)
	require.Error(t, err)
	assert.ErrorContains(t, err, retErr.Error())
	assert.Equal(t, []string{"1", "1"}, res)
	assert.NoError(t, fs.verifyClosedChannel())
}

func TestFlatterStage_ErrorOnPrevStage(t *testing.T) {
	retErr := errors.New("error on prev stage")
	s, sc := newBaseStage(func(ctx context.Context, i int) (string, error) { return "", nil })
	fs := flatterStage[int, string]{
		baseStage: s,
		fn: func(ctx context.Context, i int) ([]string, error) {
			return []string{strconv.Itoa(i), strconv.Itoa(i)}, nil
		},
	}
	fs.init()
	res, err := runData(fs.baseStage, sc, func(v int) error {
		if v == 2 {
			return retErr
		}
		return nil
	}, 1, 2, 3)
	require.Error(t, err)
	assert.ErrorIs(t, err, retErr)
	assert.Equal(t, []string{"1", "1"}, res)
	assert.NoError(t, fs.verifyClosedChannel())
}

func TestFlatterStage_NextStageIsClosed(t *testing.T) {
	s, sc := newBaseStage(func(ctx context.Context, i int) (string, error) { return "", nil })
	fs := flatterStage[int, string]{
		baseStage: s,
		fn: func(ctx context.Context, i int) ([]string, error) {
			return []string{strconv.Itoa(i), strconv.Itoa(i)}, nil
		},
	}
	fs.init()
	fs.cancelFn()
	res, err := runData(fs.baseStage, sc, noErr, 1, 2, 3)
	require.NoError(t, err)
	assert.Equal(t, []string{}, res)
	assert.NoError(t, fs.verifyClosedChannel())
}

func TestAggregatorStage_Success(t *testing.T) {
	s, sc := newBaseStage(func(ctx context.Context, i int) (AggregatedPair[string, int], error) { return nil, nil })
	as := aggregatorStage[string, int]{
		baseStage: s,
		fn: func(ctx context.Context, i int) (string, error) {
			if i%2 == 0 {
				return "even", nil
			}
			return "odd", nil
		},
	}
	as.init()
	res, err := runData(as.baseStage, sc, noErr, 1, 2, 3)
	require.NoError(t, err)
	assert.ElementsMatch(t, []AggregatedPair[string, int]{
		aggregateItem[string, int]{key: "odd", value: []int{1, 3}},
		aggregateItem[string, int]{key: "even", value: []int{2}},
	}, res)
	assert.NoError(t, as.verifyClosedChannel())
}

func TestAggregatorStage_ErrorOnStage(t *testing.T) {
	retErr := errors.New("error on stage")

	s, sc := newBaseStage(func(ctx context.Context, i int) (AggregatedPair[string, int], error) { return nil, nil })
	as := aggregatorStage[string, int]{
		baseStage: s,
		fn: func(ctx context.Context, i int) (string, error) {
			if i == 3 {
				return "", retErr
			}
			if i%2 == 0 {
				return "even", nil
			}
			return "odd", nil
		},
	}
	as.init()

	res, err := runData(as.baseStage, sc, noErr, 1, 2, 3)
	require.Error(t, err)
	assert.ErrorIs(t, err, retErr)
	assert.Empty(t, res)
	assert.NoError(t, as.verifyClosedChannel())
}

func TestAggregatorStage_PanicOnStage(t *testing.T) {
	retErr := errors.New("panic on stage")
	s, sc := newBaseStage(func(ctx context.Context, i int) (AggregatedPair[string, int], error) { return nil, nil })
	as := aggregatorStage[string, int]{
		baseStage: s,
		fn: func(ctx context.Context, i int) (string, error) {
			if i == 3 {
				panic(retErr)
			}
			if i%2 == 0 {
				return "even", nil
			}
			return "odd", nil
		},
	}
	as.init()

	res, err := runData(as.baseStage, sc, noErr, 1, 2, 3)
	require.Error(t, err)
	assert.ErrorContains(t, err, retErr.Error())
	assert.Empty(t, res)
	assert.NoError(t, as.verifyClosedChannel())
}

func TestAggregatorStage_ErrorOnPrevStage(t *testing.T) {
	retErr := errors.New("error on prev stage")
	s, sc := newBaseStage(func(ctx context.Context, i int) (AggregatedPair[string, int], error) { return nil, nil })
	as := aggregatorStage[string, int]{
		baseStage: s,
		fn: func(ctx context.Context, i int) (string, error) {
			if i%2 == 0 {
				return "even", nil
			}
			return "odd", nil
		},
	}
	as.init()
	res, err := runData(as.baseStage, sc, func(v int) error {
		if v == 3 {
			return retErr
		}
		return nil
	}, 1, 2, 3)
	require.Error(t, err)
	assert.ErrorIs(t, err, retErr)
	assert.Empty(t, res)
	assert.NoError(t, as.verifyClosedChannel())
}

func TestAggregatorStage_NextStageIsClosed(t *testing.T) {
	s, sc := newBaseStage(func(ctx context.Context, i int) (AggregatedPair[string, int], error) { return nil, nil })
	as := aggregatorStage[string, int]{
		baseStage: s,
		fn: func(ctx context.Context, i int) (string, error) {
			if i%2 == 0 {
				return "even", nil
			}
			return "odd", nil
		},
	}
	as.init()
	as.cancelFn()
	res, err := runData(as.baseStage, sc, noErr, 1, 2, 3)
	require.NoError(t, err)
	assert.Empty(t, res)
	assert.NoError(t, as.verifyClosedChannel())
}

func TestCollectorStage_Success(t *testing.T) {
	s, sc := newBaseStage(func(ctx context.Context, i int) (struct{}, error) {
		return struct{}{}, nil
	})

	cs := collectorStage[int]{
		baseStage: s,
	}
	cs.init()
	go cs.run()
	go func() {
		defer close(sc.in)
		defer close(sc.inErr)
		for _, v := range []int{1, 2, 3} {
			sc.in <- v
		}
	}()

	res, err := cs.result()
	require.NoError(t, err)
	assert.Equal(t, []int{1, 2, 3}, res)
	assert.NoError(t, cs.verifyClosedChannel())
}

func TestCollectorStage_ErrorOnPrevStage(t *testing.T) {
	retErr := errors.New("error on prev stage")
	s, sc := newBaseStage(func(ctx context.Context, i int) (struct{}, error) {
		return struct{}{}, nil
	})

	cs := collectorStage[int]{
		baseStage: s,
	}
	cs.init()
	go cs.run()
	go func() {
		defer close(sc.in)
		defer close(sc.inErr)
		sc.inErr <- retErr
	}()

	res, err := cs.result()
	require.Error(t, err)
	assert.ErrorIs(t, err, retErr)
	assert.Empty(t, res)
	assert.NoError(t, cs.verifyClosedChannel())
}

func TestParallelStage_BaseStage_Success(t *testing.T) {
	s, sc := newBaseStage(identityWithTimeout[int](time.Millisecond * 100))
	ps := newParallelStage(s, func(bs baseStage[int, int]) stage {
		return &bs
	}, 5)

	ps.setContext(s.ctx)
	ps.setCancelFunc(s.cancelFn)
	ps.init()
	res, err := runDataCtx(s.ctx, ps, sc, noErr, 1, 2, 3)
	require.NoError(t, err)
	assert.ElementsMatch(t, []int{1, 2, 3}, res)
	assert.NoError(t, ps.verifyClosedChannel())
}

func TestParallelStage_BaseStage_ErrorOnStage(t *testing.T) {
	retErr := errors.New("error on stage")
	s, sc := newBaseStage(identityWithError[int](retErr, 2))
	ps := newParallelStage(s, func(bs baseStage[int, int]) stage {
		return &bs
	}, 5)

	ps.setContext(s.ctx)
	ps.setCancelFunc(s.cancelFn)
	ps.init()
	res, err := runDataCtx(s.ctx, ps, sc, noErr, 1, 2, 3)
	require.Error(t, err)
	assert.ErrorIs(t, err, retErr)
	assert.Len(t, res, 2)
	assert.NoError(t, ps.verifyClosedChannel())
}

func TestParallelStage_BaseStage_PanicOnStage(t *testing.T) {
	retErr := errors.New("panic on stage")
	s, sc := newBaseStage(identityWithPanic[int](retErr, 2))
	ps := newParallelStage(s, func(bs baseStage[int, int]) stage {
		return &bs
	}, 5)

	ps.setContext(s.ctx)
	ps.setCancelFunc(s.cancelFn)
	ps.init()
	res, err := runDataCtx(s.ctx, ps, sc, noErr, 1, 2, 3)
	require.Error(t, err)
	assert.ErrorContains(t, err, retErr.Error())
	assert.Len(t, res, 2)
	assert.NoError(t, ps.verifyClosedChannel())
}

func TestParallelStage_BaseStage_ErrorOnPrevStage(t *testing.T) {
	retErr := errors.New("error on prev stage")
	s, sc := newBaseStage(identityWithTimeout[int](time.Millisecond * 100))
	ps := newParallelStage(s, func(bs baseStage[int, int]) stage {
		return &bs
	}, 5)

	ps.setContext(s.ctx)
	ps.setCancelFunc(s.cancelFn)
	ps.init()
	res, err := runDataCtx(s.ctx, ps, sc, func(v int) error {
		if v == 3 {
			return retErr
		}
		return nil
	}, 1, 2, 3)
	require.Error(t, err)
	assert.ErrorIs(t, err, retErr)
	assert.ElementsMatch(t, []int{1, 2}, res)
	assert.NoError(t, ps.verifyClosedChannel())
}

func TestParallelStage_BaseStage_NextStageIsClosed(t *testing.T) {
	s, sc := newBaseStage(identityWithTimeout[int](time.Millisecond * 100))
	ps := newParallelStage(s, func(bs baseStage[int, int]) stage {
		return &bs
	}, 5)

	ps.setContext(s.ctx)
	ps.setCancelFunc(s.cancelFn)
	ps.init()
	s.cancelFn()
	res, err := runDataCtx(s.ctx, ps, sc, noErr, 1, 2, 3)
	require.NoError(t, err)
	assert.Empty(t, res)
	assert.NoError(t, ps.verifyClosedChannel())
}

func TestParallelStage_TransformerStage_Success(t *testing.T) {
	s, sc := newBaseStage(identityWithTimeout[int](time.Millisecond * 100))
	ps := newParallelStage(s, func(bs baseStage[int, int]) stage {
		return &transformerStage[int, int]{
			baseStage: bs,
		}
	}, 5)

	ps.setContext(s.ctx)
	ps.setCancelFunc(s.cancelFn)
	ps.init()
	res, err := runDataCtx(s.ctx, ps, sc, noErr, 1, 2, 3)
	require.NoError(t, err)
	assert.ElementsMatch(t, []int{1, 2, 3}, res)
	assert.NoError(t, ps.verifyClosedChannel())
}

func TestParallelStage_FilterStage_Success(t *testing.T) {
	s, sc := newBaseStage(identityWithTimeout[int](time.Millisecond * 100))
	ps := newParallelStage(s, func(bs baseStage[int, int]) stage {
		return &filterStage[int]{
			baseStage: bs,
			filter: func(ctx context.Context, i int) (bool, error) {
				return i%2 == 0, nil
			},
		}
	}, 5)

	ps.setContext(s.ctx)
	ps.setCancelFunc(s.cancelFn)
	ps.init()
	res, err := runDataCtx(s.ctx, ps, sc, noErr, 1, 2, 3)
	require.NoError(t, err)
	assert.ElementsMatch(t, []int{2}, res)
	assert.NoError(t, ps.verifyClosedChannel())
}

func TestParallelStage_FlatterStage_Success(t *testing.T) {
	s, sc := newBaseStage(identityWithTimeout[int](time.Millisecond * 100))
	ps := newParallelStage(s, func(bs baseStage[int, int]) stage {
		return &flatterStage[int, int]{
			baseStage: bs,
			fn: func(ctx context.Context, i int) ([]int, error) {
				return []int{i, i}, nil
			},
		}
	}, 5)

	ps.setContext(s.ctx)
	ps.setCancelFunc(s.cancelFn)
	ps.init()
	res, err := runDataCtx(s.ctx, ps, sc, noErr, 1, 2, 3)
	require.NoError(t, err)
	assert.ElementsMatch(t, []int{1, 1, 2, 2, 3, 3}, res)
	assert.NoError(t, ps.verifyClosedChannel())
}

func identityWithTimeout[T any](timeout time.Duration) transformFn[T, T] {
	return func(ctx context.Context, t T) (T, error) {
		time.Sleep(timeout)
		return t, nil
	}
}

func identityWithError[T any](err error, iteration int) transformFn[T, T] {
	index := 0
	return func(ctx context.Context, t T) (T, error) {
		if index == iteration {
			return t, err
		}
		index++
		return t, nil
	}
}

func identityWithPanic[T any](err error, iteration int) transformFn[T, T] {
	index := 0
	return func(ctx context.Context, t T) (T, error) {
		if index == iteration {
			panic(err)
		}
		index++
		return t, nil
	}
}

func noErr[T any](T) error { return nil }

type runner interface {
	run()
}

func runData[I, O any](s baseStage[I, O], sc stageChannels[I, O], errFn func(I) error, values ...I) ([]O, error) {
	return runDataCtx(s.ctx, &s, sc, errFn, values...)
}

func runDataCtx[I, O any](ctx context.Context, s runner, sc stageChannels[I, O], errFn func(I) error, values ...I) ([]O, error) {
	result := make([]O, 0, len(values))

	var wg sync.WaitGroup

	runStage := func() {
		defer wg.Done()
		s.run()
	}
	pushValues := func() {
		defer wg.Done()
		defer func() {
			close(sc.in)
			close(sc.inErr)
		}()
		index := 0
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if index == len(values) {
					return
				}
				v := values[index]
				index++
				if err := errFn(v); err != nil {
					sc.inErr <- err
					return
				}
				select {
				case <-ctx.Done():
					return
				case sc.in <- v:
				}

			}
		}
	}
	collectValues := func() {
		defer wg.Done()
		for v := range sc.out {
			result = append(result, v)
		}
	}
	var retErr error
	collectErrors := func() {
		defer wg.Done()
		for err := range sc.outErr {
			retErr = err
		}
	}

	wg.Add(4)
	go runStage()
	go pushValues()
	go collectValues()
	go collectErrors()
	wg.Wait()

	return result, retErr
}

type stageChannels[I, O any] struct {
	inErr  chan error
	outErr chan error
	in     chan I
	out    chan O
}



func newBaseStage[I, O any](fn transformFn[I, O]) (baseStage[I, O], stageChannels[I, O]) {
	ctx, cf := context.WithCancel(context.Background())
	sc := stageChannels[I, O]{
		in:     make(chan I),
		out:    make(chan O),
		inErr:  make(chan error),
		outErr: make(chan error),
	}
	return baseStage[I, O]{
		ctx:      ctx,
		cancelFn: cf,
		name:     "new base stage",

		in:     sc.in,
		out:    sc.out,
		inErr:  sc.inErr,
		outErr: sc.outErr,

		transformationFn: fn,
	}, sc
}
