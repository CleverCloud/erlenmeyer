package prom

import (
	"reflect"
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
)

func TestProcessMatchers(t *testing.T) {
	tests := []struct {
		name            string
		matchers        []*labels.Matcher
		wantClassName   string
		wantLabels     map[string]string
	}{
		{
			name: "empty matchers",
			matchers: []*labels.Matcher{},
			wantClassName: "",
			wantLabels: map[string]string{},
		},
		{
			name: "class name only",
			matchers: []*labels.Matcher{
				{
					Name: "__name__",
					Value: "test_metric",
					Type: labels.MatchEqual,
				},
			},
			wantClassName: "test_metric",
			wantLabels: map[string]string{},
		},
		{
			name: "labels only",
			matchers: []*labels.Matcher{
				{
					Name: "env",
					Value: "prod",
					Type: labels.MatchEqual,
				},
				{
					Name: "region",
					Value: "us-west",
					Type: labels.MatchEqual,
				},
			},
			wantClassName: "",
			wantLabels: map[string]string{
				"env": "prod",
				"region": "us-west",
			},
		},
		{
			name: "class name and labels",
			matchers: []*labels.Matcher{
				{
					Name: "__name__",
					Value: "test_metric",
					Type: labels.MatchEqual,
				},
				{
					Name: "env",
					Value: "prod",
					Type: labels.MatchEqual,
				},
			},
			wantClassName: "test_metric",
			wantLabels: map[string]string{
				"env": "prod",
			},
		},
		{
			name: "regex matcher",
			matchers: []*labels.Matcher{
				{
					Name: "env",
					Value: "prod|dev",
					Type: labels.MatchRegexp,
				},
			},
			wantClassName: "",
			wantLabels: map[string]string{
				"env": "~prod|dev",
			},
		},
		{
			name: "not equal matcher",
			matchers: []*labels.Matcher{
				{
					Name: "env",
					Value: "prod",
					Type: labels.MatchNotEqual,
				},
			},
			wantClassName: "",
			wantLabels: map[string]string{
				"env": "~(?!prod).*",
			},
		},
		{
			name: "not regex matcher",
			matchers: []*labels.Matcher{
				{
					Name: "env",
					Value: "prod|dev",
					Type: labels.MatchNotRegexp,
				},
			},
			wantClassName: "",
			wantLabels: map[string]string{
				"env": "~(?!prod|dev).*",
			},
		},
		{
			name: "mixed matchers",
			matchers: []*labels.Matcher{
				{
					Name: "__name__",
					Value: "test_metric",
					Type: labels.MatchEqual,
				},
				{
					Name: "env",
					Value: "prod",
					Type: labels.MatchEqual,
				},
				{
					Name: "region",
					Value: "us.*",
					Type: labels.MatchRegexp,
				},
				{
					Name: "cluster",
					Value: "test",
					Type: labels.MatchNotEqual,
				},
			},
			wantClassName: "test_metric",
			wantLabels: map[string]string{
				"env": "prod",
				"region": "~us.*",
				"cluster": "~(?!test).*",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotClassName, gotLabels := processMatchers(tt.matchers)
			if gotClassName != tt.wantClassName {
				t.Errorf("processMatchers() className = %v, want %v", gotClassName, tt.wantClassName)
			}
			if !reflect.DeepEqual(gotLabels, tt.wantLabels) {
				t.Errorf("processMatchers() labels = %v, want %v", gotLabels, tt.wantLabels)
			}
		})
	}
}
