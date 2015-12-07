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

// GolangDoesnotAllowPointerToFloat64Literal allows one to take the address of a float64 literal
func GolangDoesnotAllowPointerToFloat64Literal(s float64) *float64 {
	return &s
}

// GolangDoesnotAllowPointerToIntLiteral allows one to take the address of a int64 literal
func GolangDoesnotAllowPointerToIntLiteral(s int64) *int64 {
	return &s
}

// GolangDoesnotAllowPointerToUint16Literal allows one to take the address of a uint16 literal
func GolangDoesnotAllowPointerToUint16Literal(s uint16) *uint16 {
	return &s
}

// GolangDoesnotAllowPointerToBooleanLiteral allows one to take the address of a bool literal
func GolangDoesnotAllowPointerToBooleanLiteral(s bool) *bool {
	return &s
}
