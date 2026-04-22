package main

import (
	"fmt"
	"os"

	"github.com/wehubfusion/Icarus/pkg/schema"
)

func main() {
	engine := schema.NewEngine()

	fmt.Println("=== JSON schema example ===")
	jsonSchema := []byte(`{
		"type": "OBJECT",
		"properties": {
			"name": { "type": "STRING", "required": true },
			"country": { "type": "STRING", "default": "USA" }
		}
	}`)
	jsonInput := []byte(`{"name":"Alice","extra_field":"will be removed"}`)
	jsonRes, err := engine.ProcessWithSchema(jsonInput, jsonSchema, schema.ProcessOptions{
		ApplyDefaults:    true,
		StructureData:    true,
		CollectAllErrors: true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "json process error: %v\n", err)
	}
	fmt.Printf("valid=%v errors=%d\n", jsonRes.Valid, len(jsonRes.Errors))
	fmt.Printf("output=%s\n\n", string(jsonRes.Data))

	fmt.Println("=== CSV schema example (input is JSON array of rows) ===")
	csvSchema := []byte(`{
		"columnHeaders": {
			"Name": { "type": "STRING" },
			"Age": { "type": "NUMBER" }
		}
	}`)
	csvRows := []byte(`[
		{"Name":"Bob","Age":42},
		{"Name":"Carol","Age":"not-a-number"}
	]`)
	csvRes, err := engine.ProcessCSVWithSchema(csvRows, csvSchema, schema.ProcessOptions{
		CollectAllErrors: true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "csv process error: %v\n", err)
	}
	fmt.Printf("valid=%v errors=%d\n", csvRes.Valid, len(csvRes.Errors))
	for _, e := range csvRes.Errors {
		fmt.Printf("[ERROR] %s %s (%s)\n", e.Path, e.Message, e.Code)
	}
	fmt.Println()

	fmt.Println("=== HL7 schema example ===")
	hl7Schema := []byte(`{
		"messageType": "ADT_A01",
		"version": "2.8",
		"segments": [
			{"name": "MSH", "usage": "R", "rpt": "1", "fields": [
				{"position": "MSH.1", "dataType": "ST", "usage": "R", "rpt": "1"},
				{"position": "MSH.2", "dataType": "ST", "usage": "R", "rpt": "1"},
				{"position": "MSH.9", "dataType": "MSG", "usage": "R", "rpt": "1"},
				{"position": "MSH.12", "dataType": "ST", "usage": "R", "rpt": "1"}
			]},
			{"name": "PID", "usage": "R", "rpt": "1", "fields": [
				{"position": "PID.3", "dataType": "CX", "usage": "R", "rpt": "1"}
			]}
		]
	}`)

	msg := []byte("MSH|^~\\&|SEND|FAC|RECV|FAC|20250305120000||ADT^A01|MSG001|P|2.5\rPID|||12345^^^NHS^NH|EXTRA")

	res, err := engine.ProcessHL7WithSchema(msg, hl7Schema, schema.ProcessOptions{
		CollectAllErrors: true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "process returned error: %v\n", err)
	}

	fmt.Printf("valid=%v\n", res.Valid)
	fmt.Printf("errors=%d warnings=%d infos=%d\n", len(res.Errors), len(res.Warnings), len(res.Infos))
	for _, e := range res.Errors {
		fmt.Printf("[ERROR] %s %s (%s)\n", e.Path, e.Message, e.Code)
	}
	for _, e := range res.Warnings {
		fmt.Printf("[WARN ] %s %s (%s)\n", e.Path, e.Message, e.Code)
	}
	for _, e := range res.Infos {
		fmt.Printf("[INFO ] %s %s (%s)\n", e.Path, e.Message, e.Code)
	}
}

