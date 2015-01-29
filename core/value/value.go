package value

import "fmt"

// A DatapointValue is the value being sent between servers.  It is usually a floating point
// but different systems support different types of values.  All values must be printable in some
// human readable interface
type DatapointValue interface {
	fmt.Stringer
}
