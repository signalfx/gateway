package flaghelpers

// StringFlag is a custom struct implementing flag.Value interface
// It is needed to detect whether a flag has actually been set or not
// since a string's nil value is "" and that could be a valid value
type StringFlag struct{ *string }

// Set implements the flag.Value interface
func (s *StringFlag) Set(in string) error {
	s.string = &in
	return nil
}

// IsSet indicates if whether the flag has actually been set or not
func (s *StringFlag) IsSet() bool {
	if s != nil && s.string != nil {
		return true
	}
	return false
}

// String implements the flag.Value interface
func (s *StringFlag) String() string {
	if s.string != nil {
		return *s.string
	}
	return ""
}

// NewStringFlag returns a new StringFlag that implements flag.Var interface.
// This allows us to check whether the string flag was actually set or not
// instead of just comparing the string to empty string
func NewStringFlag() StringFlag {
	return StringFlag{}
}
