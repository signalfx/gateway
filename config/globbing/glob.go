package globbing

import (
	"strings"

	"github.com/gobwas/glob"
)

// GetGlob takes a config and returns a gobwas Glob that has all other glob/regex operators escaped besides "*".
func GetGlob(pattern string) glob.Glob {
	patternParts := strings.Split(pattern, "*")
	for i := 0; i < len(patternParts); i++ {
		patternParts[i] = glob.QuoteMeta(patternParts[i])
	}
	return glob.MustCompile(strings.Join(patternParts, "*"))
}
