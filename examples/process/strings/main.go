package main

import (
	"fmt"
	"log"

	"github.com/wehubfusion/Icarus/pkg/process/strings"
)

func main() {
	fmt.Println("=== Icarus String Processing Utilities Demo ===")

	// Example 1: Basic string operations for workflow data preparation
	fmt.Println("1. Basic String Operations")
	fmt.Println("==========================")

	// Concatenate workflow components
	workflowID := strings.Concatenate("-", "wf", "order", "processing", "2024")
	fmt.Printf("Workflow ID: %s\n", workflowID)

	// Split and process configuration
	configStr := "env=prod,version=1.2.3,region=us-east-1"
	configParts := strings.Split(configStr, ",")
	fmt.Printf("Config parts: %v\n", configParts)

	// Join service names
	services := []string{"auth", "payment", "inventory", "shipping"}
	serviceList := strings.Join(services, " -> ")
	fmt.Printf("Service chain: %s\n", serviceList)

	// Trim and clean user input
	userInput := "  ORDER-123   "
	cleanInput := strings.Trim(userInput, "")
	fmt.Printf("Cleaned input: '%s'\n", cleanInput)

	fmt.Println()

	// Example 2: Text transformation for data normalization
	fmt.Println("2. Text Transformation")
	fmt.Println("======================")

	sampleText := "user registration data"

	// Case transformations
	fmt.Printf("Original: %s\n", sampleText)
	fmt.Printf("Upper: %s\n", strings.ToUpper(sampleText))
	fmt.Printf("Title: %s\n", strings.TitleCase(sampleText))
	fmt.Printf("Capitalize: %s\n", strings.Capitalize(sampleText))

	// Unicode normalization (removes diacritics)
	unicodeText := "café résumé naïve"
	fmt.Printf("Unicode text: %s\n", unicodeText)
	fmt.Printf("Normalized: %s\n", strings.Normalize(unicodeText))

	fmt.Println()

	// Example 3: Template formatting for dynamic messages
	fmt.Println("3. Template Formatting")
	fmt.Println("======================")

	template := "Order {orderId} for customer {customerName} has status: {status}"
	data := map[string]string{
		"orderId":      "ORD-2024-001",
		"customerName": "John Doe",
		"status":       "processing",
	}

	formattedMsg := strings.Format(template, data)
	fmt.Printf("Formatted message: %s\n", formattedMsg)

	// Also supports ${variable} syntax
	template2 := "Processing ${taskType} for ${entityType}: ${entityId}"
	data2 := map[string]string{
		"taskType":   "validation",
		"entityType": "user",
		"entityId":   "USER-456",
	}

	formattedMsg2 := strings.Format(template2, data2)
	fmt.Printf("Alternative syntax: %s\n", formattedMsg2)

	fmt.Println()

	// Example 4: Search and replace with regex for data processing
	fmt.Println("4. Search & Replace with Regex")
	fmt.Println("===============================")

	// Extract order numbers from text
	textWithOrders := "Processing orders ORD-001, ORD-002, and ORD-003 for customer CUST-123"
	orderPattern := "ORD-\\d+"
	orders, err := strings.RegexExtract(textWithOrders, orderPattern)
	if err != nil {
		log.Printf("Regex error: %v", err)
	} else {
		fmt.Printf("Found orders: %v\n", orders)
	}

	// Replace sensitive data for logging
	logMessage := "User john.doe@example.com accessed resource /api/orders/ORD-123"
	// Mask email addresses
	safeLog, err := strings.Replace(logMessage, "\\S+@\\S+\\.\\S+", "[EMAIL_MASKED]", -1, true)
	if err != nil {
		log.Printf("Replace error: %v", err)
	} else {
		fmt.Printf("Safe log: %s\n", safeLog)
	}

	fmt.Println()

	// Example 5: Encoding for secure data transmission
	fmt.Println("5. Encoding & Decoding")
	fmt.Println("======================")

	// Base64 encode sensitive workflow data
	sensitiveData := "payment_token=sk_1234567890abcdef"
	encoded := strings.Base64Encode(sensitiveData)
	fmt.Printf("Encoded data: %s\n", encoded)

	// Decode it back
	decoded, err := strings.Base64Decode(encoded)
	if err != nil {
		log.Printf("Decode error: %v", err)
	} else {
		fmt.Printf("Decoded data: %s\n", decoded)
	}

	// URL encoding for web hooks
	webhookURL := "https://api.example.com/webhooks/order/status?orderId=ORD-123&status=completed"
	encodedURL := strings.URIEncode(webhookURL)
	fmt.Printf("URL encoded: %s\n", encodedURL)

	fmt.Println()

	// Example 6: Substring operations for data extraction
	fmt.Println("6. Substring Operations")
	fmt.Println("=======================")

	transactionID := "TXN-2024-001-ABC-DEF-123"
	// Extract date part (characters 4-10: "2024-001")
	datePart := strings.Substring(transactionID, 4, 11)
	fmt.Printf("Transaction: %s\n", transactionID)
	fmt.Printf("Date part: %s\n", datePart)

	// Extract last segment (after last dash)
	lastSegment := strings.Substring(transactionID, -3, -1) // Last 3 characters
	fmt.Printf("Last segment: %s\n", lastSegment)

	fmt.Println()

	// Example 7: Validation and content checking
	fmt.Println("7. Content Validation")
	fmt.Println("=====================")

	// Check for required keywords
	documentContent := "This is a purchase order for Q4 2024 fiscal year"
	hasOrder, _ := strings.Contains(documentContent, "order", false)
	hasFiscal, _ := strings.Contains(documentContent, "fiscal", false)
	fmt.Printf("Document: %s\n", documentContent)
	fmt.Printf("Contains 'order': %v\n", hasOrder)
	fmt.Printf("Contains 'fiscal': %v\n", hasFiscal)

	// Check for valid email pattern
	emailPattern := "^[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}$"
	testEmail := "user@company.com"
	isValidEmail, _ := strings.Contains(testEmail, emailPattern, true)
	fmt.Printf("Email '%s' matches pattern: %v\n", testEmail, isValidEmail)

	fmt.Println()

	// Example 8: Workflow orchestration scenario
	fmt.Println("8. Workflow Orchestration Example")
	fmt.Println("=================================")

	// Simulate processing a customer order
	orderData := map[string]string{
		"customer": "Jane Smith",
		"product":  "premium subscription",
		"orderId":  "ORD-2024-0456",
		"amount":   "99.99",
		"currency": "USD",
		"status":   "pending",
	}

	// Create workflow steps using string processing
	steps := []string{
		strings.Format("Validate payment for {customer} - {currency} {amount}", orderData),
		strings.Format("Process order {orderId} for {product}", orderData),
		strings.Format("Send confirmation email to {customer}", orderData),
		strings.Format("Update inventory for {product}", orderData),
	}

	fmt.Println("Generated workflow steps:")
	for i, step := range steps {
		fmt.Printf("  %d. %s\n", i+1, step)
	}

	// Final status update
	finalStatus := strings.Format("Order {orderId} completed successfully for {customer}", orderData)
	fmt.Printf("\nFinal status: %s\n", finalStatus)

	fmt.Println("\n=== Demo Complete ===")
	fmt.Println("The string processing utilities provide powerful text manipulation")
	fmt.Println("capabilities essential for workflow orchestration, data processing,")
	fmt.Println("and process management in distributed systems.")
}
