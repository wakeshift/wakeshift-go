package appkit

// SetUndeclaredIsError sets whether a Subscribe the tool doesn't declare is
// refused (the next release's behavior), for the external tests; the
// returned func restores it.
func SetUndeclaredIsError(v bool) (restore func()) {
	prev := undeclaredIsError
	undeclaredIsError = v
	return func() { undeclaredIsError = prev }
}
