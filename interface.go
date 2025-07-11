package pipeline

import "context"

type TransformFunc[I, O any] transformFn[I, O]
type FilterFunc[T any] filterFn[T]
type FlatterFunc[I, O any] flatterFn[I, O]
type KeyFunc[K comparable, T any] keyFn[K, T]

type Pipeline[T any] interface {
	Run(context.Context) ([]T, error)
}

func (p *pipeline[T]) Run(ctx context.Context) ([]T, error) {
	return p.eval(ctx)
}

type AggregatedPair[K comparable, V any] interface {
	Key() K
	Values() []V
}

func (i aggregateItem[K, T]) Key() K {
	return i.key
}

func (i aggregateItem[K, T]) Values() []T {
	return i.value
}

func New[T any](data []T, opts ...Option) Pipeline[T] {
	return newPipeline(data, opts...)
}

func WithTransformer[I, O any](stageName string, p Pipeline[I], fn TransformFunc[I, O], opts ...Option) Pipeline[O] {
	return withTransformerStage(stageName, p.(*pipeline[I]), transformFn[I, O](fn), opts...)
}

func WithFilter[T any](stageName string, p Pipeline[T], fn FilterFunc[T], opts ...Option) Pipeline[T] {
	return withFilterStage(stageName, p.(*pipeline[T]), filterFn[T](fn), opts...)
}

func WithFlatter[I, O any](stageName string, p Pipeline[I], fn FlatterFunc[I, O], opts ...Option) Pipeline[O] {
	return withFlatterStage(stageName, p.(*pipeline[I]), flatterFn[I, O](fn), opts...)
}

func WithAggregator[K comparable, T any](stageName string, p Pipeline[T], fn KeyFunc[K, T], opts ...Option) Pipeline[AggregatedPair[K, T]] {
	return withAggregatorStage(stageName, p.(*pipeline[T]), keyFn[K, T](fn), opts...)
}

func Transformer[I, O any](p Pipeline[I], fn TransformFunc[I, O], opts ...Option) Pipeline[O] {
	return WithTransformer("transformation", p, fn, opts...)
}

func Filter[T any](p Pipeline[T], fn FilterFunc[T], opts ...Option) Pipeline[T] {
	return WithFilter("filtration", p, fn, opts...)
}

func Flatter[I, O any](p Pipeline[I], fn FlatterFunc[I, O], opts ...Option) Pipeline[O] {
	return WithFlatter("flatteration", p, fn, opts...)
}

func Aggregator[K comparable, T any](stageName string, p Pipeline[T], fn KeyFunc[K, T], opts ...Option) Pipeline[AggregatedPair[K, T]] {
	return WithAggregator("aggregation", p, fn, opts...)
}

func Split[T any](ctx context.Context, p Pipeline[T], count int) ([]Pipeline[T], error) {
	pipelines, err := split(ctx, p.(*pipeline[T]), count)
	if err != nil {
		return nil, err
	}
	out := make([]Pipeline[T], count)
	for i, pipeline := range pipelines {
		out[i] = pipeline
	}
	return out, nil
}

func Join[T any](ctx context.Context, pipelines ...Pipeline[T]) (Pipeline[T], error) {
	castPipelines := make([]*pipeline[T], len(pipelines))
	for i, p := range pipelines {
		castPipelines[i] = p.(*pipeline[T])
	}
	return join(ctx, castPipelines...)
}
