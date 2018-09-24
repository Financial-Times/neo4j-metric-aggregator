package concept

type Concept struct {
	UUID    string  `json:"uuid"`
	Metrics Metrics `json:"metrics"`
}

type Metrics struct {
	AnnotationsCount int64 `json:"annotationsCount"`
}
