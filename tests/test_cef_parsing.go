package main

import (
	"fmt"
	"strings"

	ceflib "github.com/ren3gadem4rm0t/cef-parser-go/parser"
	appparser "superalign.ai/internal/parser"
)

// This is the crucial preprocessing function from your application code.
// We include it here so the test can use it to clean the log line.
func preprocessFortigateCEF(line string) string {
	// 1️⃣ Find and extract the CEF portion (with CEF: prefix)
	cefIdx := strings.Index(line, "CEF:")
	if cefIdx == -1 {
		// Try with space
		cefIdx = strings.Index(line, "CEF: ")
		if cefIdx == -1 {
			// No CEF marker found, return as-is
			return line
		}
	}

	// Extract from CEF: onwards
	line = strings.TrimSpace(line[cefIdx:])

	// 2️⃣ Normalize "CEF: " to "CEF:" (space after colon)
	line = strings.Replace(line, "CEF: ", "CEF:", 1)

	// 3️⃣ Check if this is valid CEF format
	if !strings.HasPrefix(line, "CEF:") {
		return line
	}

	withoutPrefix := strings.TrimPrefix(line, "CEF:")

	// 4️⃣ Split into exactly 8 parts (Version + 7 pipe-separated fields)
	parts := strings.SplitN(withoutPrefix, "|", 8)

	// Need at least 8 parts for valid CEF
	if len(parts) < 8 {
		// Pad with empty strings if needed
		for len(parts) < 8 {
			parts = append(parts, "")
		}
		return line // Return original if not enough parts for extension processing
	}

	// 5️⃣ Normalize the Name field (index 5)
	if len(parts[5]) > 0 {
		parts[5] = strings.TrimSpace(parts[5])
		// Replace problematic characters
		parts[5] = strings.ReplaceAll(parts[5], " ", "_")
		parts[5] = strings.ReplaceAll(parts[5], ":", "_")
		parts[5] = strings.ReplaceAll(parts[5], "|", "_")
		parts[5] = strings.ReplaceAll(parts[5], "=", "_")
	}

	// 6️⃣ Clean up Severity field (index 6)
	parts[6] = strings.TrimSpace(parts[6])

	// 7️⃣ Clean up extension field (index 7)
	extension := strings.TrimSpace(parts[7])

	//
	//  THE FIX IS HERE: Manually escape the space characters within the `msg` field's value.
	//
	msgIndex := strings.Index(extension, "msg=")
	if msgIndex != -1 {
		// We need to find where the message value ends. It ends where the next key begins.
		// Let's find the end of the message by looking for the next '=' sign after "msg=".
		msgValueStartIndex := msgIndex + 4 // Start right after "msg="

		// Find the next potential key=value pair start
		searchArea := extension[msgValueStartIndex:]

		// A simple but effective way is to split the rest of the string by spaces
		// and find the first token that contains an '='.
		potentialNextTokens := strings.Split(searchArea, " ")
		var msgValueParts []string
		nextKeysAndValues := ""

		foundNextKey := false
		for i, token := range potentialNextTokens {
			if !foundNextKey && strings.Contains(token, "=") && i > 0 {
				// This is likely the start of the next key.
				foundNextKey = true
				nextKeysAndValues = strings.Join(potentialNextTokens[i:], " ")
			}
			if !foundNextKey {
				msgValueParts = append(msgValueParts, token)
			}
		}

		if foundNextKey {
			messageValue := strings.Join(msgValueParts, " ")
			// Rebuild the extension string with the space inserted before the next key
			extension = fmt.Sprintf("%smsg=%s %s", extension[:msgIndex], messageValue, nextKeysAndValues)
		}
	}

	parts[7] = extension

	// 8️⃣ Reconstruct with CEF: prefix
	result := "CEF:" + strings.Join(parts, "|")

	return result
}

func main() {
	fmt.Println("=== Testing Real FortiGate CEF Log Parsing ===")
	rawLog := `Dec 27 11:23:49 FGT-A-LOG CEF: 0|Fortinet|Fortigate|v6.0.3|13056|utm:webfilter ftgd_blk blocked|4|deviceExternalId=FGT5HD3915800610 FTNTFGTlogid=0316013056 cat=utm:webfilter FTNTFGTsubtype=webfilter FTNTFGTeventtype=ftgd_blk FTNTFGTlevel=warning FTNTFGTvd=vdom1 FTNTFGTeventtime=1545938629 FTNTFGTpolicyid=1 externalId=764 duser=bob src=10.1.100.11 spt=59194 deviceInboundInterface=port12 FTNTFGTsrcintfrole=undefined dst=185.230.61.185 dpt=80 deviceOutboundInterface=port11 FTNTFGTdstintfrole=undefined proto=6 app=HTTP dhost=ambrishsriv.wixsite.com FTNTFGTprofile=g-default act=blocked FTNTFGTreqtype=direct request=/bizsquads out=96 in=0 deviceDirection=1 msg=URL belongs to a denied category in policy FTNTFGTmethod=domain FTNTFGTcat=26 requestContext=Malicious Websites FTNTFGTcrscore=60 FTNTFGTcrlevel=high`

	fmt.Printf("\nRaw log (as received from FortiGate):\n%s\n", rawLog)
	fmt.Println("\n--------------------------------------------------------------------------------")

	// Use production preprocessing to normalize real FortiGate CEF lines
	cleanedLog := appparser.PreprocessFortigateCEF(rawLog)
	fmt.Printf("\n✅ Preprocessed Log (ready for parsing):\n%s\n", cleanedLog)
	fmt.Println("\n--------------------------------------------------------------------------------")

	// Now, parse the CLEANED log string using CEF library
	p := ceflib.NewParser(ceflib.SafeConfig())
	_, err := p.Parse(cleanedLog)

	if err != nil {
		// This should not happen now
		fmt.Printf("\n❌ Parse error: %v\n", err)
	} else {
		// Success!
		fmt.Println("\n✅ Parse successful!")
	}
}
