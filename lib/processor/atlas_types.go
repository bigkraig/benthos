package processor

import (
	"time"
)

type AtlasLog struct {
	CommandType string    `json:"command_type"`
	Hostname    string    `json:"hostname"`
	Timestamp   time.Time `json:"ts"`

	Command AtlasCommand `json:"command,omitempty"`
}

type AtlasCommand struct {
	Unparsed string         `json:"unparsed,omitempty"`
	Request  *AtlasReqResp  `json:"request,omitempty"`
	Response *AtlasReqResp  `json:"response,omitempty"`
	Error    *AtlasError    `json:"error,omitempty"`
	Opuse    *AtlasOpUse    `json:"opuse,omitempty"`
	Hostload *AtlasHostLoad `json:"hostload,omitempty"`
}

type AtlasReqResp struct {
	// response,10:52:07.630,195840581/192.168.59.44/49312/416,000000EA|
	//   result=0x0,statmsg=QUE,statcode=0x0,cache=1,mode=12,quenum=0x1,
	//   quewait=0,querate=122,sid=747152a6865a4b7ca80f1336,bid=66a4a1e1ba9e413286b38e52,cip=47.39.241.192,
	//   token=0000000108020003001B480F903AA8C00000000002550A4EEFE9E7B7;

	// response,12:03:55.460,42854398/192.168.49.220/2175/429,00000BDE|<json>

	LogTime   string             `json:"log_time,omitempty"`
	Client    *AtlasClientHeader `json:"client"`
	LengthHex string             `json:"message_length_hex,omitempty"`

	Message map[string]interface{} `json:"message,omitempty"`
}

type AtlasClientHeader struct {
	Message int    `json:"message,omitempty"`
	IP      string `json:"ip,omitempty"`
	Port    int    `json:"port,omitempty"`
	Socket  int    `json:"socket,omitempty"`
}

type AtlasError struct {
	// error,13:42:56.960,0000016A|<json>
	LogTime   string `json:"log_time,omitempty"`
	LengthHex string `json:"message_length_hex,omitempty"`

	Message map[string]interface{} `json:"message,omitempty"`
}

type AtlasOpUse struct {
	// opuse,14:54:35.450,14:54:35\tCH6\\6\\CartOps\\0\\0\\10\\8\\406347\\406361\\0\\100\\9
	LogTime string `json:"log_time"`
	VaxTime string `json:"vax_time"`

	Host     string `json:"host"`
	PortSet  int    `json:"portset"`
	Usage    string `json:"usage"`
	UsedCur  int    `json:"usedcur"`
	QueCur   int    `json:"quecur"`
	UsedPeak int    `json:"usedpeak"`
	QuePeak  int    `json:"quepeak"`
	UsedTot  int    `json:"usedtot"`
	QueTot   int    `json:"quetot"`
	Min      int    `json:"min"`
	Max      int    `json:"max"`
	Ideal    int    `json:"ideal"`
}

type AtlasHostLoad struct {
	// hostload,13:23:35.800,ARZ,0,13:23:34,29
	LogTime string `json:"log_time"`
	Vax     string `json:"vax"`
	Load    int    `json:"load"`
	VaxTime string `json:"vax_time"`
	Flags   int    `json:"flags"`
}
