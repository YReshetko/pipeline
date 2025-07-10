package pipeline

import (
	"context"
	"errors"
	"sync"
)

type pipeline[Out any] struct {
	// Originally items
	stages    []stage
	collector *collectorStage[Out]

	lastOutChan chan Out
	lastErrChan chan error

	once sync.Once
}

func (p *pipeline[Out]) eval(ctx context.Context) ([]Out, error) {
	p.once.Do(func() {
		p.setupCollector()
		p.setupStageContexts(ctx)
		p.runStages()
	})

	return p.collector.result()
}

func (p *pipeline[Out]) setupCollector() {
	cl := collectorStage[Out]{
		baseStage: baseStage[Out, struct{}]{
			name:  "collecotor",
			inErr: p.lastErrChan,
			in:    p.lastOutChan,
		},
	}
	p.collector = &cl
	p.stages = append(p.stages, &cl)
}

func (p *pipeline[Out]) setupStageContexts(ctx context.Context) {
	nextCtx, fn := context.WithCancel(ctx)
	for i := len(p.stages) - 1; i > 0; i-- {
		p.stages[i].setCancelFunc(fn)
		p.stages[i].setContext(ctx)
		ctx = nextCtx
		nextCtx, fn = context.WithCancel(nextCtx)
	}
	p.stages[0].setCancelFunc(fn)
	p.stages[0].setContext(ctx)
}

func (p *pipeline[Out]) runStages() {
	for _, stage := range p.stages {
		stage.init()
		stage.run()
	}
}

// For testing/debug purpose
func (p *pipeline[Out]) verifyClosedChannel() error {
	for _, stage := range p.stages {
		err := stage.verifyClosedChannel()
		if err != nil {
			return err
		}
	}
	return nil
}

func newPipeline[Out any](data []Out) *pipeline[Out] {
	emitterOutChan := make(chan Out)
	emitterErrChan := make(chan error)
	emitterStage := emitterStage[Out]{
		baseStage: baseStage[struct{}, Out]{
			name:   "emitter",
			outErr: emitterErrChan,
			out:    emitterOutChan,
		},
		data: data,
	}
	return &pipeline[Out]{
		stages:      []stage{&emitterStage},
		lastOutChan: emitterOutChan,
		lastErrChan: emitterErrChan,
	}
}

func withTransformerStage[I, O any](stageName string, prevPipeleine *pipeline[I], fn transformFn[I, O]) *pipeline[O] {
	return withStage(stageName, prevPipeleine, func(stg baseStage[I, O]) stage {
		stg.transformationFn = fn
		return &stg
	})
}

func withFilterStage[O any](stageName string, prevPipeleine *pipeline[O], fn filterFn[O]) *pipeline[O] {
	return withStage(stageName, prevPipeleine, func(stg baseStage[O, O]) stage {
		return &filterStage[O]{
			baseStage: stg,
			filter:    fn,
		}
	})
}

func withFlatterStage[I, O any](stageName string, prevPipeleine *pipeline[I], fn flatterFn[I, O]) *pipeline[O] {
	return withStage(stageName, prevPipeleine, func(stg baseStage[I, O]) stage {
		return &flatterStage[I, O]{
			baseStage: stg,
			fn:        fn,
		}
	})
}

func withAggregatorStage[K comparable, T any](stageName string, prevPipeleine *pipeline[T], fn keyFn[K, T]) *pipeline[AggregatedPair[K, T]] {
	return withStage(stageName, prevPipeleine, func(stg baseStage[T, AggregatedPair[K, T]]) stage {
		return &aggregatorStage[K, T]{
			baseStage: stg,
			fn:        fn,
		}
	})
}

func withStage[I, O any](stageName string, prevPipeleine *pipeline[I], stageConverter func(baseStage[I, O]) stage) *pipeline[O] {
	stg, outCh, errCh := nextBaseStage[I, O](stageName, prevPipeleine)
	return &pipeline[O]{
		stages:      append(prevPipeleine.stages, stageConverter(stg)),
		lastOutChan: outCh,
		lastErrChan: errCh,
	}
}

func nextBaseStage[I, O any](stageName string, prevPipeleine *pipeline[I]) (baseStage[I, O], chan O, chan error) {
	outCh := make(chan O)
	errCh := make(chan error)
	return baseStage[I, O]{
		name:   stageName,
		inErr:  prevPipeleine.lastErrChan,
		outErr: errCh,
		in:     prevPipeleine.lastOutChan,
		out:    outCh,
	}, outCh, errCh
}

func split[T any](ctx context.Context, prevPipeline *pipeline[T], size int) ([]*pipeline[T], error) {
	data, err := prevPipeline.eval(ctx)
	if err != nil {
		return nil, err
	}
	pipelines := make([]*pipeline[T], size)
	for i := 0; i < size; i++ {
		pipelines[i] = newPipeline(data)
	}
	return pipelines, nil
}

func join[T any](ctx context.Context, pipelines ...*pipeline[T]) (*pipeline[T], error) {
	var data []T
	var mx sync.Mutex
	writeData := func(values []T) {
		mx.Lock()
		defer mx.Unlock()
		data = append(data, values...)
	}

	var errs []error
	var mxErr sync.Mutex
	writeErr := func(err error) {
		mxErr.Lock()
		defer mxErr.Unlock()
		errs = append(errs, err)
	}

	var wg sync.WaitGroup
	wg.Add((len(pipelines)))

	evalPipeline := func(pipeline *pipeline[T]) {
		defer wg.Done()
		res, err := pipeline.eval(ctx)
		if err != nil {
			writeErr(err)
			return
		}
		writeData(res)
	}

	for _, pipeline := range pipelines {
		go evalPipeline(pipeline)
	}

	wg.Wait()

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	return newPipeline(data), nil
}
