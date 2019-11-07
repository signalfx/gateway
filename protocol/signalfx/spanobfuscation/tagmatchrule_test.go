package spanobfuscation

import (
	"testing"

	"github.com/gobwas/glob"
	"github.com/gobwas/glob/match"
	"github.com/signalfx/golib/v3/pointer"
	. "github.com/smartystreets/goconvey/convey"
)

func TestGetRules(t *testing.T) {
	defaultGlob, _ := glob.Compile(`*`)
	serviceGlob, _ := glob.Compile(`\^\\some*service\$`)
	opGlob, _ := glob.Compile(`operation\.*`)

	var cases = []struct {
		desc        string
		config      []*TagMatchRuleConfig
		outputRules []*rule
	}{
		{
			desc:        "empty service and empty operation",
			config:      []*TagMatchRuleConfig{{Tags: []string{"test-tag"}}},
			outputRules: []*rule{{service: defaultGlob, operation: defaultGlob, tags: []string{"test-tag"}}},
		},
		{
			desc:        "service regex and empty operation",
			config:      []*TagMatchRuleConfig{{Service: pointer.String(`^\some*service$`), Tags: []string{"test-tag"}}},
			outputRules: []*rule{{service: serviceGlob, operation: defaultGlob, tags: []string{"test-tag"}}},
		},
		{
			desc:        "empty service and operation regex",
			config:      []*TagMatchRuleConfig{{Operation: pointer.String(`operation.*`), Tags: []string{"test-tag"}}},
			outputRules: []*rule{{service: defaultGlob, operation: opGlob, tags: []string{"test-tag"}}},
		},
		{
			desc:        "service regex and operation regex",
			config:      []*TagMatchRuleConfig{{Service: pointer.String(`^\some*service$`), Operation: pointer.String(`operation.*`), Tags: []string{"test-tag"}}},
			outputRules: []*rule{{service: serviceGlob, operation: opGlob, tags: []string{"test-tag"}}},
		},
		{
			desc:        "multiple tags",
			config:      []*TagMatchRuleConfig{{Service: pointer.String(`^\some*service$`), Operation: pointer.String(`operation.*`), Tags: []string{"test-tag", "another-tag"}}},
			outputRules: []*rule{{service: serviceGlob, operation: opGlob, tags: []string{"test-tag", "another-tag"}}},
		},
		{
			desc: "multiple rules",
			config: []*TagMatchRuleConfig{
				{Tags: []string{"test-tag"}},
				{Service: pointer.String(`^\some*service$`), Tags: []string{"test-tag"}},
				{Operation: pointer.String(`operation.*`), Tags: []string{"test-tag"}},
				{Service: pointer.String(`^\some*service$`), Operation: pointer.String(`operation.*`), Tags: []string{"test-tag"}},
			},
			outputRules: []*rule{
				{service: defaultGlob, operation: defaultGlob, tags: []string{"test-tag"}},
				{service: serviceGlob, operation: defaultGlob, tags: []string{"test-tag"}},
				{service: defaultGlob, operation: opGlob, tags: []string{"test-tag"}},
				{service: serviceGlob, operation: opGlob, tags: []string{"test-tag"}},
			},
		},
	}
	Convey("we should create a valid SpanTagRemoval with", t, func() {
		for _, tc := range cases {
			Convey(tc.desc, func() {
				r, err := getRules(tc.config)
				So(err, ShouldBeNil)
				So(r, ShouldNotBeNil)
				for i := 0; i < len(r); i++ {
					So(r[i].service.(match.Matcher).String(), ShouldEqual, tc.outputRules[i].service.(match.Matcher).String())
					So(r[i].operation.(match.Matcher).String(), ShouldEqual, tc.outputRules[i].operation.(match.Matcher).String())
					for idx, tag := range r[i].tags {
						So(tag, ShouldEqual, tc.outputRules[i].tags[idx])
					}
				}
			})
		}
	})
}
