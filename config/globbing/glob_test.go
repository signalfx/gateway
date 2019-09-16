package globbing

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestEscapeMetaCharacters(t *testing.T) {
	var cases = []struct {
		desc    string
		pattern string
		match   []string
		noMatch []string
	}{
		{
			desc:    `^`,
			pattern: `^test*service`,
			match:   []string{`^test-service`, `^test^service`},
			noMatch: []string{`test-service`, `testservice`},
		},
		{
			desc:    `?`,
			pattern: `test?service*`,
			match:   []string{`test?service-one`},
			noMatch: []string{`test-service`, `test.service`, `testaservice`, `test.service.prod`},
		},
		{
			desc:    `\`,
			pattern: `test\*service`,
			match:   []string{`test\this\service`, `test\service`, `test\\service`},
			noMatch: []string{`test-service`, `test.service`, `testservice`},
		},
		{
			desc:    `{}`,
			pattern: `service{2}`,
			match:   []string{`service{2}`},
			noMatch: []string{`servicee`, `serviceeee`},
		},
		{
			desc:    `[]`,
			pattern: `service*[a-z]`,
			match:   []string{`service[a-z]`, `service/handle/[a-z]`},
			noMatch: []string{`servicea`, `servicee`},
		},
	}
	Convey("should correctly handle special character ", t, func() {
		for _, c := range cases {
			Convey(c.desc, func() {
				g := GetGlob(c.pattern)
				for _, m := range c.match {
					So(g.Match(m), ShouldBeTrue)
				}
				for _, n := range c.noMatch {
					So(g.Match(n), ShouldBeFalse)
				}
			})
		}
	})
}
