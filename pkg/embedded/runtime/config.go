package runtime

import "encoding/json"

// NormalizeRawConfig converts nodeSchema array format (from execution plan) to flat JSON.
// Execution plans use nodeConfig.config with structure:
//
//	{ "nodeSchema": [ { "key": "script", "value": "..." }, ... ] }
//
// Processors expect flat format: { "script": "...", ... }
// Returns the original raw bytes unchanged if config does not use nodeSchema format.
func NormalizeRawConfig(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return raw
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return raw
	}
	flat := flattenNodeSchemaConfig(m)
	if flat == nil {
		return raw
	}
	out, err := json.Marshal(flat)
	if err != nil {
		return raw
	}
	return out
}

// flattenNodeSchemaConfig converts nodeSchema array to flat key-value map.
// Returns nil if config does not use nodeSchema format.
func flattenNodeSchemaConfig(m map[string]interface{}) map[string]interface{} {
	arr, ok := m["nodeSchema"].([]interface{})
	if !ok || len(arr) == 0 {
		return nil
	}
	flat := make(map[string]interface{})
	for _, item := range arr {
		field, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		key, _ := field["key"].(string)
		if key == "" {
			continue
		}
		if val := field["value"]; val != nil {
			flat[key] = val
		}
	}
	return flat
}

// mapActionToOperation maps node schema Action (from execution plan) to processor operation.
// Aligned with Apollo seed-node-schemas.sql and nodeconfiguration_service mapActionToOperation.
// Returns empty string for unknown actions (no injection).
func mapActionToOperation(action string) string {
	switch action {
	// plugin-json-operations
	case "JSON Parser":
		return "parse"
	case "JSON Producer":
		return "produce"
	// plugin-csv-operations
	case "CSV Parse":
		return "parse"
	case "CSV Producer", "CSV Produce":
		return "produce"
	// plugin-date-formatter
	case "Date Formatter":
		return "format"
	// plugin-sftp
	case "SFTP List Files":
		return "list_files"
	case "SFTP Upload File":
		return "upload_file"
	case "SFTP Download File":
		return "download_file"
	case "SFTP Copy File":
		return "copy_file"
	case "SFTP Move File":
		return "move_file"
	case "SFTP Delete File":
		return "delete_file"
	case "SFTP Create Folder":
		return "create_folder"
	case "SFTP Move Folder":
		return "move_folder"
	case "SFTP Delete Folder":
		return "delete_folder"
	case "SFTP Append", "SFTP Append File":
		return "append_file"
	// plugin-strings
	case "String Concatenate":
		return "concatenate"
	case "String Join":
		return "join"
	case "String Split":
		return "split"
	case "String Trim":
		return "trim"
	case "String Replace":
		return "replace"
	case "String Substring":
		return "substring"
	case "String To Upper":
		return "to_upper"
	case "String To Lower":
		return "to_lower"
	case "String Title Case":
		return "title_case"
	case "String Capitalize":
		return "capitalize"
	case "String Contains":
		return "contains"
	case "String Length":
		return "length"
	case "String Regex Extract":
		return "regex_extract"
	case "String Format":
		return "format"
	case "String Base64 Encode":
		return "base64_encode"
	case "String Base64 Decode":
		return "base64_decode"
	case "String URI Encode":
		return "uri_encode"
	case "String URI Decode":
		return "uri_decode"
	case "String Normalize":
		return "normalize"
	default:
		return ""
	}
}

// InjectOperationFromAction takes normalized raw config and, when the node has an Action
// that maps to an operation, sets config["operation"] to that value so processors (jsonops,
// strings, csv, sftp, date formatter) act based on the execution plan action instead of
// the operation field in config. Other config keys are unchanged.
// If action is empty or has no mapping, returns normalizedRaw unchanged.
// When config is empty (nil or []) but action has a mapping, returns minimal {"operation": "<op>"}
// so operation-based nodes still work when config was not persisted.
func InjectOperationFromAction(normalizedRaw json.RawMessage, pluginType, action string) json.RawMessage {
	if action == "" {
		return normalizedRaw
	}
	op := mapActionToOperation(action)
	if op == "" {
		return normalizedRaw
	}
	if len(normalizedRaw) == 0 {
		out, _ := json.Marshal(map[string]interface{}{"operation": op})
		return out
	}
	var m map[string]interface{}
	if err := json.Unmarshal(normalizedRaw, &m); err != nil {
		return normalizedRaw
	}
	if m == nil {
		m = make(map[string]interface{})
	}
	m["operation"] = op
	out, err := json.Marshal(m)
	if err != nil {
		return normalizedRaw
	}
	return out
}
