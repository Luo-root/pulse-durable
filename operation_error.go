package durable

// operationError 的展示文本只含操作信息；原错误仅经 Unwrap 保留，避免泄露业务值。
type operationError struct {
	op       string
	cause    error
	panicked bool
}

func (e *operationError) Error() string {
	if e.panicked {
		return "durable: " + e.op + " panicked"
	}
	return "durable: " + e.op + " failed"
}

func (e *operationError) Unwrap() error { return e.cause }

func operationPanic(op string, value any) error {
	cause, _ := value.(error)
	return &operationError{op: op, cause: cause, panicked: true}
}
