package workarounds

import "time"

// GolangDoesnotAllowPointerToStringLiteral allows one to take the address of a string literal
func GolangDoesnotAllowPointerToStringLiteral(s string) *string {
	return &s
}

// GolangDoesnotAllowPointerToTimeLiteral allows one to take the address of a time literal
func GolangDoesnotAllowPointerToTimeLiteral(s time.Duration) *time.Duration {
	return &s
}

// GolangDoesnotAllowPointerToUintLiteral allows one to take the address of a uint32 literal
func GolangDoesnotAllowPointerToUintLiteral(s uint32) *uint32 {
	return &s
}

// GolangDoesnotAllowPointerToUintLiteral allows one to take the address of a uint32 literal
func GolangDoesnotAllowPointerToFloat64Literal(s float64) *float64 {
	return &s
}

// GolangDoesnotAllowPointerToUintLiteral allows one to take the address of a uint32 literal
func GolangDoesnotAllowPointerToIntLiteral(s int64) *int64 {
	return &s
}

// GolangDoesnotAllowPointerToUintLiteral allows one to take the address of a uint32 literal
func GolangDoesnotAllowPointerToUint16Literal(s uint16) *uint16 {
	return &s
}

// GolangDoesnotAllowPointerToUintLiteral allows one to take the address of a uint32 literal
func GolangDoesnotAllowPointerToDurationLiteral(s time.Duration) *time.Duration {
	return &s
}
