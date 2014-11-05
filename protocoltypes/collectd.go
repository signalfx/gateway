package protocoltypes

// CollectdJSONWriteBody is the full POST body of collectd's write_http format
type CollectdJSONWriteBody []CollectdJSONWriteFormat

// CollectdJSONWriteFormat is the format for collectd json datapoints
type CollectdJSONWriteFormat struct {
	Dsnames        []*string  `json:"dsnames"`
	Dstypes        []*string  `json:"dstypes"`
	Host           *string    `json:"host"`
	Interval       *float64   `json:"interval"`
	Plugin         *string    `json:"plugin"`
	PluginInstance *string    `json:"plugin_instance"`
	Time           *float64   `json:"time"`
	TypeS          *string    `json:"type"`
	TypeInstance   *string    `json:"type_instance"`
	Values         []*float64 `json:"values"`
}
