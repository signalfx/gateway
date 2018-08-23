package idtool

import (
	"encoding/base64"
	"encoding/binary"
	"strings"
)

// GetIDAsString returns the string representation every other sfx service uses of an int64
func GetIDAsString(id int64) string {
	b := make([]byte, 8)
	return GetIDAsStringWithBuffer(id, b)
}

// GetIDAsStringWithBuffer returns the string representation every other sfx service uses of an int64
// but takes the correct size 8 byte buffer
func GetIDAsStringWithBuffer(id int64, buff []byte) string {
	if len(buff) != 8 {
		return ""
	}
	binary.BigEndian.PutUint64(buff, uint64(id))
	return strings.TrimRight(base64.URLEncoding.EncodeToString(buff), "=")
}

// GetStringAsID returns the int64 for a given string, if it's wacky, returns 0
func GetStringAsID(idstr string) int64 {
	if idstr != "" {
		if idstr[len(idstr)-1] != '=' {
			idstr = idstr + "="
		}
		buff, err := base64.URLEncoding.DecodeString(idstr)
		if err == nil {
			output := binary.BigEndian.Uint64(buff)
			return int64(output)
		}
	}
	return 0
}
