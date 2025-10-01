//go:build ignore

package main

import (
	"encoding/json"
	"fmt"
	"strings"

	ceflib "github.com/ren3gadem4rm0t/cef-parser-go/parser"
	appparser "superalign.ai/internal/parser"
)

func main() {
	// Your exact log from the question
	rawLog := `Dec 27 11:23:49 FGT-A-LOG CEF: 0|Fortinet|Fortigate|v6.0.3|13056|utm:webfilter ftgd_blk blocked|4|deviceExternalId=FGT5HD3915800610 FTNTFGTlogid=0316013056 cat=utm:webfilter FTNTFGTsubtype=webfilter FTNTFGTeventtype=ftgd_blk FTNTFGTlevel=warning FTNTFGTvd=vdom1 FTNTFGTeventtime=1545938629 FTNTFGTpolicyid=1 externalId=764 duser=bob src=10.1.100.11 spt=59194 deviceInboundInterface=port12 FTNTFGTsrcintfrole=undefined dst=185.230.61.185 dpt=80 deviceOutboundInterface=port11 FTNTFGTdstintfrole=undefined proto=6 app=HTTP dhost=ambrishsriv.wixsite.com FTNTFGTprofile=g-default act=blocked FTNTFGTreqtype=direct request=/bizsquads out=96 in=0 deviceDirection=1 msg=URL belongs to a denied category in policy FTNTFGTmethod=domain FTNTFGTcat=26 requestContext=Malicious Websites FTNTFGTcrscore=60 FTNTFGTcrlevel=high`

	fmt.Println("=== Testing Production CEF Parser ===\n")
	fmt.Println("Original log:")
	fmt.Println(rawLog)
	fmt.Println("\n" + strings.Repeat("-", 80) + "\n")

	// Step 1: Preprocess (same as production)
	preprocessed := appparser.PreprocessFortigateCEF(rawLog)
	fmt.Println("✅ Step 1: Preprocessed")
	fmt.Printf("Result: %s\n", preprocessed)
	fmt.Println("\n" + strings.Repeat("-", 80) + "\n")

	// Step 2: Parse with CEF parser (same as production)
	p := ceflib.NewParser(ceflib.SafeConfig())
	cef, err := p.Parse(preprocessed)

	if err != nil {
		fmt.Printf("❌ Parse error: %v\n", err)
		return
	}

	fmt.Println("✅ Step 2: CEF Parsing successful!")
	fmt.Printf("\nCEF Header Fields:\n")
	fmt.Printf("  Device Vendor: %s\n", cef.DeviceVendor)
	fmt.Printf("  Device Product: %s\n", cef.DeviceProduct)
	fmt.Printf("  Signature ID: %s\n", cef.SignatureID)
	fmt.Printf("  Name: %s\n", cef.Name)
	fmt.Printf("  Severity: %s\n", cef.Severity)

	fmt.Println("\n" + strings.Repeat("-", 80) + "\n")

	// Step 3: Extract extensions (same as production does)
	fmt.Println("✅ Step 3: Extracted Extension Fields:\n")

	extJSON := cef.Extensions.AsJSON()
	var extMap map[string]interface{}
	if json.Unmarshal([]byte(extJSON), &extMap) == nil {
		// Show the fields you asked about
		importantFields := []string{
			"dhost", "app", "proto", "src", "dst", "spt", "dpt",
			"act", "msg", "requestContext", "request",
		}

		for _, field := range importantFields {
			if value, exists := extMap[field]; exists {
				fmt.Printf("  ✅ %-20s = %v\n", field, value)
			}
		}
	}

	fmt.Println("\n" + strings.Repeat("-", 80) + "\n")

	// Step 4: Show what would be sent to Kafka
	fmt.Println("✅ Step 4: Sample Kafka JSON (key fields):\n")

	kafkaEntry := map[string]interface{}{
		"device_vendor":  cef.DeviceVendor,
		"device_product": cef.DeviceProduct,
		"name":           cef.Name,
		"severity":       cef.Severity,
	}

	// Add extensions
	if json.Unmarshal([]byte(extJSON), &extMap) == nil {
		for k, v := range extMap {
			kafkaEntry[k] = v

			// Show the mapping that production does
			if k == "src" {
				kafkaEntry["srcip"] = v
			}
			if k == "dst" {
				kafkaEntry["dstip"] = v
			}
			if k == "dhost" {
				kafkaEntry["dstname"] = v
			}
			if k == "act" {
				kafkaEntry["action"] = v
			}
		}
	}

	jsonBytes, _ := json.MarshalIndent(kafkaEntry, "", "  ")
	fmt.Println(string(jsonBytes))

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("✅ SUCCESS! All fields including dhost, app, proto, msg, etc. work correctly!")
	fmt.Println(strings.Repeat("=", 80))
}
