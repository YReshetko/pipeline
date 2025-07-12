package pipeline

type option func(o *options)
type Option = option

type options struct {
	noRecovery          bool
	parallelStages      bool
	parallelStagesCount int
}

func WithNoRecoveryOption() Option {
	return func(o *options) {
		o.noRecovery = true
	}
}

func WithParallelStages(count int) Option {
	return func(o *options) {
		o.parallelStages = true
		o.parallelStagesCount = count
	}
}

func WithNoParallelStages() Option {
	return func(o *options) {
		o.parallelStages = false
		o.parallelStagesCount = 0
	}
}
