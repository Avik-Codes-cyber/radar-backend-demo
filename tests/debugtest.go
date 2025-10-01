

import (
	"fmt"
	"strings"

	"github.com/ren3gadem4rm0t/cef-parser-go/parser"
)

func main() {
	rawLog := `Dec 27 11:23:49 FGT-A-LOG CEF: 0|Fortinet|Fortigate|v6.0.3|13056|utm:webfilter ftgd_blk blocked|4|deviceExternalId=FGT5HD3915800610 FTNTFGTlogid=0316013056 cat=utm:webfilter FTNTFGTsubtype=webfilter FTNTFGTeventtype=ftgd_blk FTNTFGTlevel=warning FTNTFGTvd=vdom1 FTNTFGTeventtime=1545938629 FTNTFGTpolicyid=1 externalId=764 duser=bob src=10.1.100.11 spt=59194 deviceInboundInterface=port12 FTNTFGTsrcintfrole=undefined dst=185.230.61.185 dpt=80 deviceOutboundInterface=port11 FTNTFGTdstintfrole=undefined proto=6 app=HTTP dhost=ambrishsriv.wixsite.com FTNTFGTprofile=g-default act=blocked FTNTFGTreqtype=direct request=/bizsquads out=96 in=0 deviceDirection=1 msg=URL belongs to a denied category in policy FTNTFGTmethod=domain FTNTFGTcat=26 requestContext=Malicious Websites FTNTFGTcrscore=60 FTNTFGTcrlevel=high`

	fmt.Println("=== Step-by-Step CEF Preprocessing Debug ===\n")
	fmt.Printf("Original log:\n%s\n\n", rawLog)

	// Step 1: Find CEF
	cefIdx := strings.Index(rawLog, "CEF:")
	fmt.Printf("Step 1 - CEF: index: %d\n", cefIdx)
	if cefIdx == -1 {
		fmt.Println("❌ CEF: not found!")
		return
	}

	extracted := strings.TrimSpace(rawLog[cefIdx:])
	fmt.Printf("Step 1 - Extracted from CEF:\n%s\n\n", extracted)

	// Step 2: Normalize
	normalized := strings.Replace(extracted, "CEF: ", "CEF:", 1)
	fmt.Printf("Step 2 - After normalizing 'CEF: ' to 'CEF:':\n%s\n\n", normalized)

	// Step 3: Split
	withoutPrefix := strings.TrimPrefix(normalized, "CEF:")
	parts := strings.SplitN(withoutPrefix, "|", 7)

	fmt.Printf("Step 3 - Split into %d parts:\n", len(parts))
	for i, part := range parts {
		fieldName := []string{"Version", "Device Vendor", "Device Product", "Device Version", "Signature ID", "Name", "Extension"}[i]
		display := part
		if len(display) > 80 {
			display = display[:80] + "..."
		}
		fmt.Printf("  [%d] %s: %q\n", i, fieldName, display)
	}
	fmt.Println()

	// Step 4: Fix Name field
	originalName := parts[5]
	parts[5] = strings.TrimSpace(parts[5])
	parts[5] = strings.ReplaceAll(parts[5], " ", "_")
	parts[5] = strings.ReplaceAll(parts[5], ":", "_")

	fmt.Printf("Step 4 - Name field transformation:\n")
	fmt.Printf("  Before: %q\n", originalName)
	fmt.Printf("  After:  %q\n\n", parts[5])

	// Step 5: Reconstruct
	reconstructed := "CEF:" + strings.Join(parts, "|")
	fmt.Printf("Step 5 - Reconstructed CEF:\n%s\n\n", reconstructed)

	// Step 6: Try parsing
	fmt.Println("Step 6 - Attempting to parse with CEF parser...")
	safeParser := parser.NewParser(parser.SafeConfig())
	cef, err := safeParser.Parse(reconstructed)

	if err != nil {
		fmt.Printf("❌ Parse error: %v\n\n", err)

		// Try to diagnose
		fmt.Println("Diagnostic checks:")
		fmt.Printf("  - Starts with 'CEF:': %v\n", strings.HasPrefix(reconstructed, "CEF:"))
		fmt.Printf("  - Number of pipes: %d (expected: 6)\n", strings.Count(reconstructed, "|"))
		fmt.Printf("  - Version field: %q (expected: number like '0')\n", parts[0])

		// Show first 200 chars
		if len(reconstructed) > 200 {
			fmt.Printf("\nFirst 200 chars of reconstructed:\n%s...\n", reconstructed[:200])
		}
	} else {
		fmt.Println("✅ Parse successful!")
		fmt.Printf("\nParsed CEF object:\n")
		fmt.Printf("  Device Vendor: %s\n", cef.DeviceVendor)
		fmt.Printf("  Device Product: %s\n", cef.DeviceProduct)
		fmt.Printf("  Version: %s\n", cef.Version)
		fmt.Printf("  Signature ID: %s\n", cef.SignatureID)
		fmt.Printf("  Name: %s\n", cef.Name)
		fmt.Printf("  Severity: %s\n", cef.Severity)
		fmt.Printf("  Extensions: %d fields\n", len(cef.Extensions.AsMap()))
	}
}
