package concept

type Concept struct {
	UUID    string  `json:"uuid"`
	Metrics Metrics `json:"metrics"`
}

type Metrics struct {
	AnnotationsCount Stats `json:"annotationsCount"`
}

type Stats struct {
	RecentCount int64 `json:"recent"`
	TotalCount  int64 `json:"total"`
}
