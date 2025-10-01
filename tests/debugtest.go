package main

import (
	"fmt"
	"strings"

	"github.com/ren3gadem4rm0t/cef-parser-go/parser"
	appparser "superalign.ai/internal/parser"
)

func main() {
	rawLog := `Dec 27 11:23:49 FGT-A-LOG CEF: 0|Fortinet|Fortigate|v6.0.3|13056|utm:webfilter ftgd_blk blocked|4|deviceExternalId=FGT5HD3915800610 FTNTFGTlogid=0316013056 cat=utm:webfilter FTNTFGTsubtype=webfilter FTNTFGTeventtype=ftgd_blk FTNTFGTlevel=warning FTNTFGTvd=vdom1 FTNTFGTeventtime=1545938629 FTNTFGTpolicyid=1 externalId=764 duser=bob src=10.1.100.11 spt=59194 deviceInboundInterface=port12 FTNTFGTsrcintfrole=undefined dst=185.230.61.185 dpt=80 deviceOutboundInterface=port11 FTNTFGTdstintfrole=undefined proto=6 app=HTTP dhost=ambrishsriv.wixsite.com FTNTFGTprofile=g-default act=blocked FTNTFGTreqtype=direct request=/bizsquads out=96 in=0 deviceDirection=1 msg=URL belongs to a denied category in policy FTNTFGTmethod=domain FTNTFGTcat=26 requestContext=Malicious Websites FTNTFGTcrscore=60 FTNTFGTcrlevel=high`

	fmt.Println("=== CEF Preprocessing Debug ===\n")
	fmt.Printf("Original log (as received from FortiGate):\n%s\n\n", rawLog)
	fmt.Println("--------------------------------------------------------------------------------")

	// Use the production preprocessing function
	preprocessed := appparser.PreprocessFortigateCEF(rawLog)
	fmt.Printf("\n✅ Preprocessed CEF (ready for parsing):\n%s\n\n", preprocessed)
	fmt.Println("--------------------------------------------------------------------------------")

	// Show structure breakdown
	withoutPrefix := strings.TrimPrefix(preprocessed, "CEF:")
	parts := strings.SplitN(withoutPrefix, "|", 8)

	fmt.Printf("\nCEF Structure (%d parts):\n", len(parts))
	for i, part := range parts {
		fieldName := []string{"Version", "Device Vendor", "Device Product", "Device Version", "Signature ID", "Name", "Severity", "Extension"}[i]
		display := part
		if len(display) > 80 {
			display = display[:80] + "..."
		}
		fmt.Printf("  [%d] %s: %q\n", i, fieldName, display)
	}
	fmt.Println()

	// Check if msg field is in the extension
	if len(parts) > 7 {
		extension := parts[7]
		msgIdx := strings.Index(extension, "msg=")
		if msgIdx != -1 {
			fmt.Println("DEBUG: Found msg field at index", msgIdx)
			// Show the area around msg
			endIdx := msgIdx + 100
			if endIdx > len(extension) {
				endIdx = len(extension)
			}
			fmt.Printf("DEBUG: Extension around msg field:\n%s\n\n", extension[msgIdx:endIdx])
		}
	}

	// Parse the preprocessed CEF
	fmt.Println("Attempting to parse with CEF parser...")
	safeParser := parser.NewParser(parser.SafeConfig())
	cef, err := safeParser.Parse(preprocessed)

	if err != nil {
		fmt.Printf("\n❌ Parse error: %v\n\n", err)

		// Diagnostic information
		fmt.Println("Diagnostic checks:")
		fmt.Printf("  - Starts with 'CEF:': %v\n", strings.HasPrefix(preprocessed, "CEF:"))
		fmt.Printf("  - Number of pipes: %d\n", strings.Count(preprocessed, "|"))
		fmt.Printf("  - Version field: %q\n", parts[0])

		// Show first 200 chars
		if len(preprocessed) > 200 {
			fmt.Printf("\nFirst 200 chars:\n%s...\n", preprocessed[:200])
		}
	} else {
		fmt.Println("\n✅ Parse successful!")
		fmt.Printf("\nParsed CEF object:\n")
		fmt.Printf("  Device Vendor: %s\n", cef.DeviceVendor)
		fmt.Printf("  Device Product: %s\n", cef.DeviceProduct)
		fmt.Printf("  Version: %s\n", cef.Version)
		fmt.Printf("  Signature ID: %s\n", cef.SignatureID)
		fmt.Printf("  Name: %s\n", cef.Name)
		fmt.Printf("  Severity: %s\n", cef.Severity)
		fmt.Printf("  Extensions: %d fields\n", len(cef.Extensions.AsMap()))

		// Show some key extensions
		extMap := cef.Extensions.AsMap()
		if msg, ok := extMap["msg"]; ok {
			fmt.Printf("  Message: %s\n", msg)
		}
		if src, ok := extMap["src"]; ok {
			fmt.Printf("  Source IP: %s\n", src)
		}
		if dst, ok := extMap["dst"]; ok {
			fmt.Printf("  Dest IP: %s\n", dst)
		}
	}
}
