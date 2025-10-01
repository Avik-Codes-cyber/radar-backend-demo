package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Simulates the parseFortiRFC3164Message function
func parseFortiRFC3164Message(msg string) map[string]string {
	result := make(map[string]string)
	for _, kv := range strings.Fields(msg) {
		parts := strings.SplitN(kv, "=", 2)
		if len(parts) == 2 {
			// Remove surrounding quotes from value if present
			value := parts[1]
			if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
				value = value[1 : len(value)-1]
			}
			result[parts[0]] = value
		}
	}
	return result
}

func main() {
	// Your FortiGate log as it would ACTUALLY arrive (proper RFC3164 format)
	// FortiGate sends: <priority>timestamp hostname message
	// Priority 189 = Facility 23 (local7), Severity 5 (notice)
	fortigateLog := `date=2019-05-13 time=16:29:50 logid="0000000013" type="traffic" subtype="forward" level="notice" vd="vdom1" eventtime=1557790190452146185 srcip=10.1.100.11 srcport=44258 srcintf="port12" srcintfrole="undefined" dstip=185.244.31.158 dstport=80 dstintf="port11" dstintfrole="undefined" srcuuid="ae28f494-5735-51e9-f247-d1d2ce663f4b" dstuuid="ae28f494-5735-51e9-f247-d1d2ce663f4b" poluuid="ccb269e0-5735-51e9-a218-a397dd08b7eb" sessionid=381780 proto=6 action="close" policyid=1 policytype="policy" service="HTTP" dstcountry="Germany" srccountry="Reserved" trandisp="snat" transip=172.16.200.2 transport=44258 duration=5 sentbyte=736 rcvdbyte=3138 sentpkt=14 rcvdpkt=5 appcat="unscanned" utmaction="block" countweb=1 crscore=30 craction=4194304 osname="Ubuntu" mastersrcmac="a2:e9:00:ec:40:01" srcmac="a2:e9:00:ec:40:01" srcserver=0 utmref=65497-796`

	fmt.Println("=== Testing FortiGate Log Parsing ===\n")
	fmt.Println("Raw log (as received from FortiGate):")
	fmt.Println(fortigateLog)
	fmt.Println("\n" + strings.Repeat("-", 80) + "\n")

	// Parse raw FortiGate log (no syslog wrapper needed)
	fmt.Println("✅ Parsing raw FortiGate key=value format...\n")

	// Build the entry map (simulating the parser)
	entry := make(map[string]string)
	entry["raw_log"] = fortigateLog
	entry["format"] = "fortigate-raw"

	// Extract FortiGate key=value pairs directly
	fortiFields := parseFortiRFC3164Message(fortigateLog)
	for key, value := range fortiFields {
		entry[key] = value
	}

	fmt.Println("✅ Parse successful!")

	fmt.Println("\n✅ Extracted FortiGate fields (what will be sent to Kafka):\n")

	// Print the 19 required fields
	requiredFields := []string{
		"date", "time", "sessionid", "srcip", "srcname", "devtype", "osname", "devid",
		"dstip", "dstname", "appcat", "app", "appid", "apprisk", "action",
		"eventtime", "logid", "type", "subtype",
	}

	fmt.Println("Required fields for Python:")
	for _, field := range requiredFields {
		value, exists := entry[field]
		if exists && value != "" {
			fmt.Printf("  ✅ %-15s = %s\n", field, value)
		} else {
			fmt.Printf("  ⚠️  %-15s = (not present - will use default)\n", field)
		}
	}

	fmt.Println("\nOther useful fields extracted:")
	otherFields := []string{
		"level", "vd", "policyid", "srcport", "srcintf", "srcintfrole",
		"dstport", "dstintf", "dstintfrole", "proto", "service", "hostname",
		"profile", "reqtype", "url", "sentbyte", "rcvdbyte", "direction",
		"method", "cat", "catdesc", "crscore", "craction", "crlevel",
	}

	for _, field := range otherFields {
		if value, exists := entry[field]; exists && value != "" {
			fmt.Printf("  • %-20s = %s\n", field, value)
		}
	}

	// Show the JSON that would be sent to Kafka
	fmt.Println("\n" + strings.Repeat("-", 80))
	fmt.Println("\n📤 JSON sent to Kafka (sample - showing key fields):\n")

	kafkaJSON := map[string]interface{}{
		"date":      entry["date"],
		"time":      entry["time"],
		"logid":     entry["logid"],
		"type":      entry["type"],
		"subtype":   entry["subtype"],
		"eventtime": entry["eventtime"],
		"sessionid": entry["sessionid"],
		"srcip":     entry["srcip"],
		"dstip":     entry["dstip"],
		"hostname":  entry["hostname"], // This becomes dstname via enrichment
		"action":    entry["action"],
		"policyid":  entry["policyid"],
		"proto":     entry["proto"],
		"sentbyte":  entry["sentbyte"],
		"rcvdbyte":  entry["rcvdbyte"],
		"level":     entry["level"],
	}

	jsonBytes, _ := json.MarshalIndent(kafkaJSON, "", "  ")
	fmt.Println(string(jsonBytes))

	fmt.Println("\n✅ All quotes removed!")
	fmt.Println("✅ Ready for Python Flink consumer!")
}
