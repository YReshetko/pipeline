package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	_ stage = (*baseStage[struct{}, struct{}])(nil)
	_ stage = (*collectorStage[struct{}])(nil)
	_ stage = (*emitterStage[struct{}])(nil)
	_ stage = (*transformerStage[struct{}, struct{}])(nil)
	_ stage = (*filterStage[struct{}])(nil)
	_ stage = (*flatterStage[struct{}, struct{}])(nil)
	_ stage = (*aggregatorStage[struct{}, struct{}])(nil)
	_ stage = (*parallelStage[struct{}, struct{}])(nil)
)

// Testing purpose only
const verifyClosedChannelTimeout = time.Second

var errSkip = errors.New("skip")

type stageFn[In, Out any] func(context.Context, In) (Out, error)

type transformFn[I, O any] stageFn[I, O]
type filterFn[I any] stageFn[I, bool]
type flatterFn[I, O any] stageFn[I, []O]
type keyFn[K comparable, T any] stageFn[T, K]

type stageBuilder[I, O any] func(baseStage[I, O]) stage

type aggregateItem[K comparable, T any] struct {
	key   K
	value []T
}

type stage interface {
	setContext(context.Context)
	setCancelFunc(context.CancelFunc)

	init()
	run()

	// For debug/testing purpose
	verifyClosedChannel() error
}

type state func() state

func runStateMachine(start state) {
	go func() {
		current := start
		for current != nil {
			current = current()
		}
	}()
}

type baseStage[I, O any] struct {
	ctx      context.Context
	cancelFn context.CancelFunc
	name     string

	inErr  <-chan error
	outErr chan<- error
	in     <-chan I
	out    chan<- O

	opts options

	// state machine arguments
	inputFinishedInterceptor state
	transformationFn         transformFn[I, O]
	input                    I
	inputReceived            bool
	chErr                    error
	errReceived              bool
	output                   O
	outputErr                error

	// start state and can be replaced to stop waiting for error from closed input channel
	currentWaitState state
}

func (s *baseStage[I, O]) init() {
	s.currentWaitState = s.waitState
}

func (s *baseStage[I, O]) run() {
	runStateMachine(s.currentWaitState)
}

func (s *baseStage[I, O]) waitState() state {
	select {
	case <-s.ctx.Done():
		return s.completeState
	case s.input, s.inputReceived = <-s.in:
		return s.handleInputReceivedState
	case s.chErr, s.errReceived = <-s.inErr:
		return s.handleErrorReceivedState
	}
}

func (s *baseStage[I, O]) waitNoErrorState() state {
	select {
	case <-s.ctx.Done():
		return s.completeState
	case s.input, s.inputReceived = <-s.in:
		return s.handleInputReceivedState
	}
}

func (s *baseStage[I, O]) handleInputReceivedState() state {
	switch {
	case s.inputReceived:
		return s.handleTransformerState
	case s.inputFinishedInterceptor != nil:
		return s.inputFinishedInterceptor
	default:
		return s.completeState
	}
}

func (s *baseStage[I, O]) handleErrorReceivedState() state {
	switch {
	case s.errReceived:
		return s.transferErrorState
	default:
		s.currentWaitState = s.waitNoErrorState
		return s.currentWaitState
	}
}

func (s *baseStage[I, O]) handleTransformerState() (retState state) {
	if !s.opts.noRecovery {
		defer func() {
			if rec := recover(); rec != nil {
				s.outputErr = fmt.Errorf("wrap panic to error: %s", rec)
				retState = s.sendTransformationErrorState
			}
		}()
	}
	s.output, s.outputErr = s.transformationFn(s.ctx, s.input)
	switch {
	case s.outputErr == nil:
		return s.sendValueState
	case errors.Is(s.outputErr, errSkip):
		return s.currentWaitState
	default:
		return s.sendTransformationErrorState
	}
}

func (s *baseStage[I, O]) sendValueState() state {
	select {
	case s.out <- s.output:
		return s.currentWaitState
	case <-s.ctx.Done():
		return s.completeState
	}
}

func (s *baseStage[I, O]) sendTransformationErrorState() state {
	select {
	case s.outErr <- fmt.Errorf("error on stage '%s': %w", s.name, s.outputErr):
		return s.completeState
	case <-s.ctx.Done():
		return s.completeState
	}
}

func (s *baseStage[I, O]) transferErrorState() state {
	s.outErr <- s.chErr
	return s.completeState
}

func (s *baseStage[I, O]) completeState() state {
	s.cancelFn()
	close(s.out)
	close(s.outErr)
	// clean up in channels since we call s.cancelFn we are sure the previouse stage has closed out channels, so in channeld of this stage are the same
	for range s.in {
	}
	for range s.inErr {
	}
	return nil
}

func (s *baseStage[I, O]) setContext(ctx context.Context) {
	s.ctx = ctx
}

func (s *baseStage[I, O]) setCancelFunc(fn context.CancelFunc) {
	s.cancelFn = fn
}

// For test/debug purpose only
func (s *baseStage[I, O]) verifyClosedChannel() error {
	return errors.Join(
		verifyChanClosed(fmt.Sprintf("in channel is not closed for stage: %s", s.name), s.in),
		verifyChanClosed(fmt.Sprintf("in err channel is not closed for stage: %s", s.name), s.inErr),
		verifyChanClosed(fmt.Sprintf("context is not closed for stage: %s", s.name), s.ctx.Done()),
	)
}

// For test/debug purpose only
func verifyChanClosed[T any](msg string, ch <-chan T) error {
	if ch == nil {
		return nil
	}
	ticker := time.NewTicker(verifyClosedChannelTimeout)
	select {
	case v, ok := <-ch:
		if ok {
			return fmt.Errorf(msg+": has element: %+v", v)
		}
		return nil
	case <-ticker.C:
		return fmt.Errorf("%s: is blocked more than %d ms", msg, verifyClosedChannelTimeout.Milliseconds())
	}
}

type emitterStage[T any] struct {
	baseStage[struct{}, T]
	data []T

	valuesCh chan T
}

func (s *emitterStage[T]) init() {
	ch := make(chan struct{}, len(s.data))
	s.valuesCh = make(chan T, len(s.data))
	defer func() {
		close(ch)
		close(s.valuesCh)
	}()
	for _, value := range s.data {
		s.valuesCh <- value
		ch <- struct{}{}
	}

	s.in = ch
	s.currentWaitState = s.waitNoErrorState
	s.transformationFn = s.transformation
}

func (s *emitterStage[T]) transformation(context.Context, struct{}) (T, error) {
	return <-s.valuesCh, nil
}

// For test/debug purpose
func (s *emitterStage[T]) verifyClosedChannel() error {
	return errors.Join(
		// Do not verify in channel as this could be non-empty, but it is definitely closed, because this is the major condition to stop pipeline
		verifyChanClosed(fmt.Sprintf("in err channel is not closed for stage: %s", s.name), s.inErr),
		verifyChanClosed(fmt.Sprintf("context is not closed for stage: %s", s.name), s.ctx.Done()),
	)
}

type transformerStage[I, O any] struct {
	baseStage[I, O]
}

type filterStage[T any] struct {
	baseStage[T, T]
	filter filterFn[T]
}

func (s *filterStage[T]) init() {
	s.transformationFn = s.transformation
	s.currentWaitState = s.waitState
}

func (s *filterStage[T]) transformation(ctx context.Context, value T) (T, error) {
	var zeroValue T
	ok, err := s.filter(ctx, value)
	if err != nil {
		return zeroValue, err
	}
	if ok {
		return value, nil
	}
	return zeroValue, errSkip
}

type flatterStage[I, O any] struct {
	baseStage[I, O]
	fn flatterFn[I, O]
}

func (s *flatterStage[I, O]) init() {
	s.transformationFn = s.transformation
	s.currentWaitState = s.waitState
}

func (s *flatterStage[I, O]) transformation(ctx context.Context, value I) (O, error) {
	var zeroValue O
	arr, err := s.fn(ctx, value)
	if err != nil {
		return zeroValue, err
	}

	for _, v := range arr {
		select {
		case <-s.ctx.Done():
			return zeroValue, nil
		case s.out <- v:
		}
	}
	return zeroValue, errSkip
}

type aggregatorStage[K comparable, T any] struct {
	baseStage[T, AggregatedPair[K, T]]
	fn keyFn[K, T]

	aggregaedValues map[K][]T
	pairs           []AggregatedPair[K, T]
}

func (s *aggregatorStage[K, T]) init() {
	s.aggregaedValues = make(map[K][]T)
	s.inputFinishedInterceptor = s.handlePairsState
	s.transformationFn = s.transformation
	s.currentWaitState = s.waitState
}

func (s *aggregatorStage[K, T]) transformation(ctx context.Context, value T) (AggregatedPair[K, T], error) {
	var zeroValue aggregateItem[K, T]
	key, err := s.fn(ctx, value)
	if err != nil {
		return zeroValue, err
	}
	s.aggregaedValues[key] = append(s.aggregaedValues[key], value)
	return zeroValue, errSkip
}

func (s *aggregatorStage[K, T]) handlePairsState() state {
	if len(s.aggregaedValues) == 0 {
		return s.completeState
	}
	s.pairs = make([]AggregatedPair[K, T], len(s.aggregaedValues))
	var index int
	for k, v := range s.aggregaedValues {
		s.pairs[index] = aggregateItem[K, T]{key: k, value: v}
		index++
	}
	return s.sendPairState
}

func (s *aggregatorStage[K, T]) sendPairState() state {
	if len(s.pairs) == 0 {
		return s.completeState
	}

	var value AggregatedPair[K, T]
	value, s.pairs = s.pairs[0], s.pairs[1:]
	select {
	case s.out <- value:
		return s.sendPairState
	case <-s.ctx.Done():
		return s.completeState
	}
}

type collectorStage[I any] struct {
	baseStage[I, struct{}]
	data []I
	err  error

	remOutCh chan struct{}
	remErrCh chan error
}

func (s *collectorStage[I]) init() {
	s.remOutCh = make(chan struct{})
	s.remErrCh = make(chan error)

	s.out = s.remOutCh
	s.outErr = s.remErrCh

	s.transformationFn = s.transformation
	s.currentWaitState = s.waitState
}

func (s *collectorStage[I]) transformation(_ context.Context, value I) (struct{}, error) {
	s.data = append(s.data, value)
	return struct{}{}, nil
}

func (s *collectorStage[I]) result() ([]I, error) {
	var wg sync.WaitGroup
	wg.Add(2)
	go s.ensureElementsReceived(&wg)
	go s.ensureErrorsReceived(&wg)
	wg.Wait()
	return s.data, s.err
}

func (s *collectorStage[I]) ensureElementsReceived(wg *sync.WaitGroup) {
	defer wg.Done()
	for range s.remOutCh {
	}
}

func (s *collectorStage[I]) ensureErrorsReceived(wg *sync.WaitGroup) {
	defer wg.Done()
	for err := range s.remErrCh {
		if err != nil {
			s.err = err
		}
	}
}

// For test/debug purpose
func (s *collectorStage[I]) verifyClosedChannel() error {
	return errors.Join(
		verifyChanClosed(fmt.Sprintf("in channel is not closed on stage: %s", s.name), s.in),
		verifyChanClosed(fmt.Sprintf("in error channel is not closed on stage: %s", s.name), s.inErr),
		// Do not verify collector context, as this is an origin context from caller and could not be closed over there
		verifyChanClosed(fmt.Sprintf("out  channel is not closed on stage: %s", s.name), s.remOutCh),
		verifyChanClosed(fmt.Sprintf("out error channel is not closed on stage: %s", s.name), s.remErrCh),
	)
}

type parallelStageItem[I, O any] struct {
	baseStage[I, O]
	stage stage

	out    chan O
	outErr chan error
	sctx   context.Context
}

func (s parallelStageItem[I, O]) transmitValue(wg *sync.WaitGroup) {
	defer wg.Done()
	for v := range s.out {
		s.baseStage.out <- v
	}
}

func (s parallelStageItem[I, O]) transmitError(wg *sync.WaitGroup) {
	defer wg.Done()
	for v := range s.outErr {
		s.baseStage.outErr <- v
	}
}

func (s parallelStageItem[I, O]) handleDone(wg *sync.WaitGroup) {
	defer wg.Done()
	<-s.sctx.Done()
}

type parallelStage[I, O any] struct {
	// The stage that is embedded into all other stages of this one
	// actually this stage is unused for now
	baseStage[I, O]
	stages []*parallelStageItem[I, O]
}

func newParallelStage[I, O any](bs baseStage[I, O], stageProducer stageBuilder[I, O], count int) stage {
	stages := make([]*parallelStageItem[I, O], count)
	for i := range stages {
		outCh := make(chan O)
		errCh := make(chan error)
		sbs := bs // copy baseStage to replace out channels and contexts
		sbs.out = outCh
		sbs.outErr = errCh
		stages[i] = &parallelStageItem[I, O]{
			baseStage: bs, // original base stage
			stage:     stageProducer(sbs),
			out:       outCh,
			outErr:    errCh,
		}
	}
	return &parallelStage[I, O]{
		baseStage: bs,
		stages:    stages,
	}
}

func (s *parallelStage[I, O]) init() {
	for _, s := range s.stages {
		s.stage.init()
	}
}

func (s *parallelStage[I, O]) run() {
	for _, s := range s.stages {
		s.stage.run()
	}
	var wg sync.WaitGroup
	count := len(s.stages)
	wg.Add(count * 3)
	for _, stg := range s.stages {
		go stg.transmitValue(&wg)
		go stg.transmitError(&wg)
		go stg.handleDone(&wg)
	}
	go func() {
		wg.Wait()
		s.baseStage.completeState()
	}()
}

func (s *parallelStage[I, O]) verifyClosedChannel() error {
	var errs []error
	for i, s := range s.stages {
		err := s.stage.verifyClosedChannel()
		if err != nil {
			errs = append(errs, fmt.Errorf("error on stage of parallel stage '%d': %w", i, err))
		}
		err = s.baseStage.verifyClosedChannel()
		if err != nil {
			errs = append(errs, fmt.Errorf("error on baseStage of parallel stage '%d': %w", i, err))
		}
	}
	if err := s.baseStage.verifyClosedChannel(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (s *parallelStage[I, O]) setContext(ctx context.Context) {
	s.baseStage.setContext(ctx)
	for _, cs := range s.stages {
		sctx, sfn := context.WithCancel(ctx)
		cs.stage.setContext(sctx)
		cs.stage.setCancelFunc(sfn)
		cs.baseStage.setContext(sctx)
		cs.sctx = sctx
	}
}
func (s *parallelStage[I, O]) setCancelFunc(fn context.CancelFunc) {
	s.baseStage.setCancelFunc(fn)
}
