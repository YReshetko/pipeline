package pipeline

type option func(o *options)
type Option = option

type options struct {
	noRecovery bool
}

func WithNoRecoveryOption() Option {
	return func(o *options) {
		o.noRecovery = true
	}
}
