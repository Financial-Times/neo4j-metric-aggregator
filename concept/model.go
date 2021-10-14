package concept

type Concept struct {
	UUID    string  `json:"uuid"`
	Metrics Metrics `json:"metrics"`
}

type Metrics struct {
	AnnotationsCount         int64 `json:"annotationsCount"`
	PrevWeekAnnotationsCount int64 `json:"prevWeekAnnotationsCount"`
}

type NeoMetricResult struct {
	UUID        string `json:"uuid"`
	RecentCount int64  `json:"recentCount"`
	TotalCount  int64  `json:"totalCount"`
}
