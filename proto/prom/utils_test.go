package prom

import (
	"encoding/json"
	"testing"

	"github.com/ovh/erlenmeyer/core"
)

func TestWarpToPrometheusResponseInstantScalar(t *testing.T) {
	gtss := []core.GeoTimeSeries{{
		Class:  "scalar",
		Values: [][]interface{}{{4000000.0, "2.0"}},
	}}

	resp, err := warpToPrometheusResponseInstant(gtss, "1 + 1")
	if err != nil {
		t.Fatal(err)
	}

	got, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}

	want := `{"resultType":"scalar","result":[4,"2.0"]}`
	if string(got) != want {
		t.Errorf("got %s, want %s", got, want)
	}
}
