package pipeline

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"

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

	assert.Equal(t, []int{}, res)
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

func noErr[T any](T) error { return nil }

func runData[I, O any](s baseStage[I, O], sc stageChannels[I, O], errFn func(I) error, values ...I) ([]O, error) {
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
			case <-s.ctx.Done():
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
				case <-s.ctx.Done():
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
