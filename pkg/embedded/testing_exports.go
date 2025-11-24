package embedded

// DetectAutoIterationForTests exposes detectAutoIteration for external test packages.
func DetectAutoIterationForTests(p *Processor, input []byte) (bool, []interface{}, error) {
	return p.detectAutoIteration(input)
}

// SetFieldAtPathForTests exposes setFieldAtPath for external test packages.
func SetFieldAtPathForTests(fm *FieldMapper, data map[string]interface{}, path string, value interface{}) {
	fm.setFieldAtPath(data, path, value)
}
