package spanobfuscation

import (
	"fmt"
	"github.com/gobwas/glob"
	"github.com/signalfx/gateway/config/globbing"
)

type rule struct {
	service   glob.Glob
	operation glob.Glob
	tags      []string
}

// TagMatchRuleConfig describes a wildcard search for a service and operation, along with which specific tags to match
// Service and Operation can both include "*" for wildcard search, but Tags will only perform an exact text match
// If Service or Operation are omitted, they will use a default value of "*", to match any service/operation
// Tags must be present, and cannot be empty
type TagMatchRuleConfig struct {
	Service   *string  `json:",omitempty"`
	Operation *string  `json:",omitempty"`
	Tags      []string `json:",omitempty"`
}

func getRules(ruleConfigs []*TagMatchRuleConfig) ([]*rule, error) {
	var rules []*rule
	for _, r := range ruleConfigs {
		service := "*"
		if r.Service != nil {
			service = *r.Service
		}
		operation := "*"
		if r.Operation != nil {
			operation = *r.Operation
		}
		if len(r.Tags) == 0 {
			return nil, fmt.Errorf("must include Tags for %s:%s", service, operation)
		}

		serviceGlob := globbing.GetGlob(service)
		operationGlob := globbing.GetGlob(operation)
		for _, t := range r.Tags {
			if t == "" {
				return nil, fmt.Errorf("found empty tag in %s:%s", service, operation)
			}
		}

		rules = append(rules,
			&rule{
				service:   serviceGlob,
				operation: operationGlob,
				tags:      r.Tags,
			})
	}
	return rules, nil
}
