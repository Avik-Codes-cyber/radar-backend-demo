package models

import (
	"time"
)

// CEFLogEntry represents a parsed CEF log entry
type CEFLogEntry struct {
	DeviceVendor  string    `json:"device_vendor"`
	DeviceProduct string    `json:"device_product"`
	SignatureID   string    `json:"signature_id"`
	Severity      int       `int:"severity"`
	Src           string    `json:"src"`
	Dst           string    `json:"dst"`
	Name          string    `json:"name"`
	Timestamp     time.Time `json:"timestamp"`
	RawLog        string    `json:"raw_log"`
}

type CSVLogEntry struct {
	Timestamp                 string `parquet:"name=timestamp, type=BYTE_ARRAY, convertedtype=UTF8" json:"timestamp"`
	Source                    string `parquet:"name=source, type=BYTE_ARRAY, convertedtype=UTF8" json:"source"`
	Message                   string `parquet:"name=message, type=BYTE_ARRAY, convertedtype=UTF8" json:"message"`
	Header1DeviceVendor       string `parquet:"name=header1_devicevendor, type=BYTE_ARRAY, convertedtype=UTF8" json:"header1_deviceVendor"`
	Header2DeviceProduct      string `parquet:"name=header2_deviceproduct, type=BYTE_ARRAY, convertedtype=UTF8" json:"header2_deviceProduct"`
	Header4DeviceEventClassId string `parquet:"name=header4_deviceeventclassid, type=BYTE_ARRAY, convertedtype=UTF8" json:"header4_deviceEventClassId"`
	Header5Name               string `parquet:"name=header5_name, type=BYTE_ARRAY, convertedtype=UTF8" json:"header5_name"`
	Duser                     string `parquet:"name=duser, type=BYTE_ARRAY, convertedtype=UTF8" json:"duser"`
	Cn1                       string `parquet:"name=cn1, type=BYTE_ARRAY, convertedtype=UTF8" json:"cn1"`
	Dpt                       string `parquet:"name=dpt, type=BYTE_ARRAY, convertedtype=UTF8" json:"dpt"`
	Dst                       string `parquet:"name=dst, type=BYTE_ARRAY, convertedtype=UTF8" json:"dst"`
	Out                       string `parquet:"name=out, type=BYTE_ARRAY, convertedtype=UTF8" json:"out"`
	Act                       string `parquet:"name=act, type=BYTE_ARRAY, convertedtype=UTF8" json:"act"`
	EventId                   string `parquet:"name=eventid, type=BYTE_ARRAY, convertedtype=UTF8" json:"eventId"`
	Type                      string `parquet:"name=type, type=BYTE_ARRAY, convertedtype=UTF8" json:"type"`
	Start                     string `parquet:"name=start, type=BYTE_ARRAY, convertedtype=UTF8" json:"start"`
	End                       string `parquet:"name=end, type=BYTE_ARRAY, convertedtype=UTF8" json:"end"`
	App                       string `parquet:"name=app, type=BYTE_ARRAY, convertedtype=UTF8" json:"app"`
	Proto                     string `parquet:"name=proto, type=BYTE_ARRAY, convertedtype=UTF8" json:"proto"`
	Art                       string `parquet:"name=art, type=BYTE_ARRAY, convertedtype=UTF8" json:"art"`
	Cat                       string `parquet:"name=cat, type=BYTE_ARRAY, convertedtype=UTF8" json:"cat"`
	DeviceSeverity            string `parquet:"name=deviceseverity, type=BYTE_ARRAY, convertedtype=UTF8" json:"deviceSeverity"`
	Rt                        string `parquet:"name=rt, type=BYTE_ARRAY, convertedtype=UTF8" json:"rt"`
	Src                       string `parquet:"name=src, type=BYTE_ARRAY, convertedtype=UTF8" json:"src"`
	SourceZoneURI             string `parquet:"name=sourcezoneuri, type=BYTE_ARRAY, convertedtype=UTF8" json:"sourceZoneURI"`
	Spt                       string `parquet:"name=spt, type=BYTE_ARRAY, convertedtype=UTF8" json:"spt"`
	Dhost                     string `parquet:"name=dhost, type=BYTE_ARRAY, convertedtype=UTF8" json:"dhost"`
	DestinationZoneURI        string `parquet:"name=destinationzoneuri, type=BYTE_ARRAY, convertedtype=UTF8" json:"destinationZoneURI"`
	Request                   string `parquet:"name=request, type=BYTE_ARRAY, convertedtype=UTF8" json:"request"`
	RequestContext            string `parquet:"name=requestcontext, type=BYTE_ARRAY, convertedtype=UTF8" json:"requestContext"`
	RequestMethod             string `parquet:"name=requestmethod, type=BYTE_ARRAY, convertedtype=UTF8" json:"requestMethod"`
	Ahost                     string `parquet:"name=ahost, type=BYTE_ARRAY, convertedtype=UTF8" json:"ahost"`
	Agt                       string `parquet:"name=agt, type=BYTE_ARRAY, convertedtype=UTF8" json:"agt"`
	DeviceExternalId          string `parquet:"name=deviceexternalid, type=BYTE_ARRAY, convertedtype=UTF8" json:"deviceExternalId"`
	DeviceInboundInterface    string `parquet:"name=deviceinboundinterface, type=BYTE_ARRAY, convertedtype=UTF8" json:"deviceInboundInterface"`
	DeviceOutboundInterface   string `parquet:"name=deviceoutboundinterface, type=BYTE_ARRAY, convertedtype=UTF8" json:"deviceOutboundInterface"`
	Suser                     string `parquet:"name=suser, type=BYTE_ARRAY, convertedtype=UTF8" json:"suser"`
	Shost                     string `parquet:"name=shost, type=BYTE_ARRAY, convertedtype=UTF8" json:"shost"`
	Reason                    string `parquet:"name=reason, type=BYTE_ARRAY, convertedtype=UTF8" json:"reason"`
	RequestClientApplication  string `parquet:"name=requestclientapplication, type=BYTE_ARRAY, convertedtype=UTF8" json:"requestClientApplication"`
	Cnt                       string `parquet:"name=cnt, type=BYTE_ARRAY, convertedtype=UTF8" json:"cnt"`
	Amac                      string `parquet:"name=amac, type=BYTE_ARRAY, convertedtype=UTF8" json:"amac"`
	Atz                       string `parquet:"name=atz, type=BYTE_ARRAY, convertedtype=UTF8" json:"atz"`
	Dvchost                   string `parquet:"name=dvchost, type=BYTE_ARRAY, convertedtype=UTF8" json:"dvchost"`
	Dtz                       string `parquet:"name=dtz, type=BYTE_ARRAY, convertedtype=UTF8" json:"dtz"`
	Dvc                       string `parquet:"name=dvc, type=BYTE_ARRAY, convertedtype=UTF8" json:"dvc"`
	DeviceZoneURI             string `parquet:"name=devicezoneuri, type=BYTE_ARRAY, convertedtype=UTF8" json:"deviceZoneURI"`
	DeviceFacility            string `parquet:"name=devicefacility, type=BYTE_ARRAY, convertedtype=UTF8" json:"deviceFacility"`
	ExternalId                string `parquet:"name=externalid, type=BYTE_ARRAY, convertedtype=UTF8" json:"externalId"`
	Msg                       string `parquet:"name=msg, type=BYTE_ARRAY, convertedtype=UTF8" json:"msg"`
	At                        string `parquet:"name=at, type=BYTE_ARRAY, convertedtype=UTF8" json:"at"`
	CategorySignificance      string `parquet:"name=categorysignificance, type=BYTE_ARRAY, convertedtype=UTF8" json:"categorySignificance"`
	CategoryBehavior          string `parquet:"name=categorybehavior, type=BYTE_ARRAY, convertedtype=UTF8" json:"categoryBehavior"`
	CategoryDeviceGroup       string `parquet:"name=categorydevicegroup, type=BYTE_ARRAY, convertedtype=UTF8" json:"categoryDeviceGroup"`
	Catdt                     string `parquet:"name=catdt, type=BYTE_ARRAY, convertedtype=UTF8" json:"catdt"`
	CategoryOutcome           string `parquet:"name=categoryoutcome, type=BYTE_ARRAY, convertedtype=UTF8" json:"categoryOutcome"`
	CategoryObject            string `parquet:"name=categoryobject, type=BYTE_ARRAY, convertedtype=UTF8" json:"categoryObject"`
	Cs1                       string `parquet:"name=cs1, type=BYTE_ARRAY, convertedtype=UTF8" json:"cs1"`
	Cs2                       string `parquet:"name=cs2, type=BYTE_ARRAY, convertedtype=UTF8" json:"cs2"`
	Cs3                       string `parquet:"name=cs3, type=BYTE_ARRAY, convertedtype=UTF8" json:"cs3"`
	Cs4                       string `parquet:"name=cs4, type=BYTE_ARRAY, convertedtype=UTF8" json:"cs4"`
	Cs5                       string `parquet:"name=cs5, type=BYTE_ARRAY, convertedtype=UTF8" json:"cs5"`
	Cs6                       string `parquet:"name=cs6, type=BYTE_ARRAY, convertedtype=UTF8" json:"cs6"`
	Cs1Label                  string `parquet:"name=cs1label, type=BYTE_ARRAY, convertedtype=UTF8" json:"cs1Label"`
	Cs2Label                  string `parquet:"name=cs2label, type=BYTE_ARRAY, convertedtype=UTF8" json:"cs2Label"`
	Cs3Label                  string `parquet:"name=cs3label, type=BYTE_ARRAY, convertedtype=UTF8" json:"cs3Label"`
	Cs4Label                  string `parquet:"name=cs4label, type=BYTE_ARRAY, convertedtype=UTF8" json:"cs4Label"`
	Cs5Label                  string `parquet:"name=cs5label, type=BYTE_ARRAY, convertedtype=UTF8" json:"cs5Label"`
	Cs6Label                  string `parquet:"name=cs6label, type=BYTE_ARRAY, convertedtype=UTF8" json:"cs6Label"`
	Cn1Label                  string `parquet:"name=cn1label, type=BYTE_ARRAY, convertedtype=UTF8" json:"cn1Label"`
	Cn2                       string `parquet:"name=cn2, type=BYTE_ARRAY, convertedtype=UTF8" json:"cn2"`
	Cn2Label                  string `parquet:"name=cn2label, type=BYTE_ARRAY, convertedtype=UTF8" json:"cn2Label"`
	Cn3                       string `parquet:"name=cn3, type=BYTE_ARRAY, convertedtype=UTF8" json:"cn3"`
	Cn3Label                  string `parquet:"name=cn3label, type=BYTE_ARRAY, convertedtype=UTF8" json:"cn3Label"`
	Cn4                       string `parquet:"name=cn4, type=BYTE_ARRAY, convertedtype=UTF8" json:"cn4"`
	Cn4Label                  string `parquet:"name=cn4label, type=BYTE_ARRAY, convertedtype=UTF8" json:"cn4Label"`
	Cn5                       string `parquet:"name=cn5, type=BYTE_ARRAY, convertedtype=UTF8" json:"cn5"`
	Cn5Label                  string `parquet:"name=cn5label, type=BYTE_ARRAY, convertedtype=UTF8" json:"cn5Label"`
	Cn6                       string `parquet:"name=cn6, type=BYTE_ARRAY, convertedtype=UTF8" json:"cn6"`
	Cn6Label                  string `parquet:"name=cn6label, type=BYTE_ARRAY, convertedtype=UTF8" json:"cn6Label"`
	C6a2Label                 string `parquet:"name=c6a2label, type=BYTE_ARRAY, convertedtype=UTF8" json:"c6a2Label"`
	C6a3Label                 string `parquet:"name=c6a3label, type=BYTE_ARRAY, convertedtype=UTF8" json:"c6a3Label"`
	C6a2                      string `parquet:"name=c6a2, type=BYTE_ARRAY, convertedtype=UTF8" json:"c6a2"`
	C6a3                      string `parquet:"name=c6a3, type=BYTE_ARRAY, convertedtype=UTF8" json:"c6a3"`
	DeviceProcessName         string `parquet:"name=deviceprocessname, type=BYTE_ARRAY, convertedtype=UTF8" json:"deviceProcessName"`
	Duid                      string `parquet:"name=duid, type=BYTE_ARRAY, convertedtype=UTF8" json:"duid"`
	Suid                      string `parquet:"name=suid, type=BYTE_ARRAY, convertedtype=UTF8" json:"suid"`
	Spid                      string `parquet:"name=spid, type=BYTE_ARRAY, convertedtype=UTF8" json:"spid"`
	Dproc                     string `parquet:"name=dproc, type=BYTE_ARRAY, convertedtype=UTF8" json:"dproc"`
	Sproc                     string `parquet:"name=sproc, type=BYTE_ARRAY, convertedtype=UTF8" json:"sproc"`
	Outcome                   string `parquet:"name=outcome, type=BYTE_ARRAY, convertedtype=UTF8" json:"outcome"`
	DestinationServiceName    string `parquet:"name=destinationservicename, type=BYTE_ARRAY, convertedtype=UTF8" json:"destinationServiceName"`
	Dpriv                     string `parquet:"name=dpriv, type=BYTE_ARRAY, convertedtype=UTF8" json:"dpriv"`
	OldFileId                 string `parquet:"name=oldfileid, type=BYTE_ARRAY, convertedtype=UTF8" json:"oldFileId"`
	OldFileHash               string `parquet:"name=oldfilehash, type=BYTE_ARRAY, convertedtype=UTF8" json:"oldFileHash"`
	Fname                     string `parquet:"name=fname, type=BYTE_ARRAY, convertedtype=UTF8" json:"fname"`
	FileId                    string `parquet:"name=fileid, type=BYTE_ARRAY, convertedtype=UTF8" json:"fileId"`
	FileType                  string `parquet:"name=filetype, type=BYTE_ARRAY, convertedtype=UTF8" json:"fileType"`
	SourceTranslatedAddress   string `parquet:"name=sourcetranslatedaddress, type=BYTE_ARRAY, convertedtype=UTF8" json:"sourceTranslatedAddress"`
	SourceTranslatedPort      string `parquet:"name=sourcetranslatedport, type=BYTE_ARRAY, convertedtype=UTF8" json:"sourceTranslatedPort"`
	In                        string `parquet:"name=in, type=BYTE_ARRAY, convertedtype=UTF8" json:"in"`
	Smac                      string `parquet:"name=smac, type=BYTE_ARRAY, convertedtype=UTF8" json:"smac"`
	Dmac                      string `parquet:"name=dmac, type=BYTE_ARRAY, convertedtype=UTF8" json:"dmac"`
	DeviceDirection           string `parquet:"name=devicedirection, type=BYTE_ARRAY, convertedtype=UTF8" json:"deviceDirection"`
	Dntdom                    string `parquet:"name=dntdom, type=BYTE_ARRAY, convertedtype=UTF8" json:"dntdom"`
	DeviceTranslatedAddress   string `parquet:"name=devicetranslatedaddress, type=BYTE_ARRAY, convertedtype=UTF8" json:"deviceTranslatedAddress"`
	DstName                   string `parquet:"name=dstname, type=BYTE_ARRAY, convertedtype=UTF8" json:"dstName"`
	DestinationName           string `parquet:"name=destinationname, type=BYTE_ARRAY, convertedtype=UTF8" json:"destinationName"`
	URL                       string `parquet:"name=url, type=BYTE_ARRAY, convertedtype=UTF8" json:"url"`
}

// AITool represents the structure of the AI tools table in Supabase
type AITool struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	URL         string `json:"url"`
	Content     string `json:"content"`
	HasAI       bool   `json:"has_ai"`
	AICategory  string `json:"ai_category"`
	RedirectURL string `json:"redirect_url"`
	IconURL     string `json:"icon_url"`
}
