package pipeline

import (
	"context"
	"errors"
	"sync"
)

type pipeline[Out any] struct {
	stages    []stage
	collector *collectorStage[Out]

	opts options

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
			opts:  p.opts,
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

func newPipeline[Out any](data []Out, opts ...option) *pipeline[Out] {
	options := options{}
	for _, o := range opts {
		o(&options)
	}
	emitterOutChan := make(chan Out)
	emitterErrChan := make(chan error)
	emitterStage := emitterStage[Out]{
		baseStage: baseStage[struct{}, Out]{
			name:   "emitter",
			outErr: emitterErrChan,
			out:    emitterOutChan,
			opts:   options,
		},
		data: data,
	}
	return &pipeline[Out]{
		stages:      []stage{&emitterStage},
		lastOutChan: emitterOutChan,
		lastErrChan: emitterErrChan,
		opts:        options,
	}
}

func withTransformerStage[I, O any](stageName string, prevPipeleine *pipeline[I], fn transformFn[I, O], opts ...option) *pipeline[O] {
	return withStage(stageName, prevPipeleine, func(stg baseStage[I, O]) stage {
		ts := transformerStage[I, O]{
			baseStage: stg,
		}
		ts.transformationFn = fn
		return &ts
	}, opts...)
}

func withFilterStage[O any](stageName string, prevPipeleine *pipeline[O], fn filterFn[O], opts ...option) *pipeline[O] {
	return withStage(stageName, prevPipeleine, func(stg baseStage[O, O]) stage {
		return &filterStage[O]{
			baseStage: stg,
			filter:    fn,
		}
	}, opts...)
}

func withFlatterStage[I, O any](stageName string, prevPipeleine *pipeline[I], fn flatterFn[I, O], opts ...option) *pipeline[O] {
	return withStage(stageName, prevPipeleine, func(stg baseStage[I, O]) stage {
		return &flatterStage[I, O]{
			baseStage: stg,
			fn:        fn,
		}
	}, opts...)
}

func withAggregatorStage[K comparable, T any](stageName string, prevPipeleine *pipeline[T], fn keyFn[K, T], opts ...option) *pipeline[AggregatedPair[K, T]] {
	return withStage(stageName, prevPipeleine, func(stg baseStage[T, AggregatedPair[K, T]]) stage {
		return &aggregatorStage[K, T]{
			baseStage: stg,
			fn:        fn,
		}
	}, append(opts, WithNoParallelStages())...)
}

func withStage[I, O any](stageName string, prevPipeleine *pipeline[I], stageConverter stageBuilder[I, O], opts ...option) *pipeline[O] {
	stg, outCh, errCh := nextBaseStage[I, O](stageName, prevPipeleine, opts...)
	var nextStage stage
	if stg.opts.parallelStages {
		nextStage = newParallelStage(stg, stageConverter, stg.opts.parallelStagesCount)
	} else {
		nextStage = stageConverter(stg)
	}
	return &pipeline[O]{
		stages:      append(prevPipeleine.stages, nextStage),
		lastOutChan: outCh,
		lastErrChan: errCh,
		opts:        prevPipeleine.opts,
	}
}

func nextBaseStage[I, O any](stageName string, prevPipeleine *pipeline[I], opts ...option) (baseStage[I, O], chan O, chan error) {
	outCh := make(chan O)
	errCh := make(chan error)
	so := prevPipeleine.opts
	for _, o := range opts {
		o(&so)
	}
	return baseStage[I, O]{
		name:   stageName,
		inErr:  prevPipeleine.lastErrChan,
		outErr: errCh,
		in:     prevPipeleine.lastOutChan,
		out:    outCh,
		opts:   so,
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
		pipelines[i].opts = prevPipeline.opts
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

	pipeline := newPipeline(data)
	if len(pipelines) > 0 {
		pipeline.opts = pipelines[0].opts
	}

	return pipeline, nil
}
