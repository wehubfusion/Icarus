package celhl7

// CELRule is the user-authored custom validation rule stored in HL7 schema JSON.
type CELRule struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Scope       string `json:"scope,omitempty"`
	When        string `json:"when,omitempty"`
	Assert      string `json:"assert,omitempty"`
	Require     string `json:"require,omitempty"`
	Forbid      string `json:"forbid,omitempty"`
	Message     string `json:"message,omitempty"`
	ErrorPath   string `json:"errorPath,omitempty"`
	Severity    string `json:"severity,omitempty"`
}
