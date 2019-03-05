package processor

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/response"
	"github.com/Jeffail/benthos/lib/types"
)

func init() {
	Constructors[TypeAtlas] = TypeSpec{
		constructor: NewAtlas,
		description: `
Parses the Atlas log type. Not all commands are supported, if its unsupported the unparsed data is thrown into ` + "`command.unparsed`" + `.`,
	}
}

type AtlasConfig struct {
	Parts []int `json:"parts" yaml:"parts"`
}

func NewAtlasConfig() AtlasConfig {
	return AtlasConfig{
		Parts: []int{},
	}
}

type Atlas struct {
	conf  AtlasConfig
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mSucc      metrics.StatCounter
	mErr       metrics.StatCounter
	mSkipped   metrics.StatCounter
	mSent      metrics.StatCounter
	mSentParts metrics.StatCounter
}

func NewAtlas(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	return &Atlas{
		conf:  conf.Atlas,
		log:   log.NewModule(".processor.atlas"),
		stats: stats,

		mCount:     stats.GetCounter("processor.atlas.count"),
		mSucc:      stats.GetCounter("processor.atlas.success"),
		mErr:       stats.GetCounter("processor.atlas.error"),
		mSkipped:   stats.GetCounter("processor.atlas.skipped"),
		mSent:      stats.GetCounter("processor.atlas.sent"),
		mSentParts: stats.GetCounter("processor.atlas.parts.sent"),
	}, nil
}

func (a *Atlas) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	a.mCount.Incr(1)

	newMsg := msg.Copy()

	targetParts := a.conf.Parts
	if len(targetParts) == 0 {
		targetParts = make([]int, newMsg.Len())
		for i := range targetParts {
			targetParts[i] = i
		}
	}

	for _, index := range targetParts {
		part := msg.Get(index).Get()
		if string(part) == "null" { // how does this happen
			continue
		}
		part = part[1 : len(part)-1] // strip leading/trailing quotes
		if part[len(part)-1] == ';' {
			part = part[:len(part)-1]
		}

		newPart, err := a.parse(part)
		if err == nil {
			a.mSucc.Incr(1)
			newMsg.Get(index).Set(newPart)
		} else {
			a.log.Errorf("Failed to parse message part: %v\n", err)
			a.mErr.Incr(1)
		}
	}

	if newMsg.Len() == 0 {
		a.mSkipped.Incr(1)
		return nil, response.NewAck()
	}

	a.mSent.Incr(1)
	a.mSentParts.Incr(int64(newMsg.Len()))
	msgs := [1]types.Message{newMsg}
	return msgs[:], nil
}

func (a *Atlas) parse(msg []byte) (parsedMsg []byte, err error) {
	// Aug 22 15:18:32 atl2.shared.phx2.websys.tmcs atlas: syncwait.*
	tsMsg := string(msg)[0:15]
	sMsg := string(msg)[16:]

	parts := strings.SplitN(sMsg, " ", 3)

	var atlasLog *AtlasLog
	if len(parts) != 3 {
		return nil, fmt.Errorf("Invalid number of parts in `%s`", msg)
	}

	switch {
	case strings.HasPrefix(parts[2], "response,"):
		fallthrough
	case strings.HasPrefix(parts[2], "request,"):
		atlasLog, err = a.parseResponse(parts[2])
	case strings.HasPrefix(parts[2], "error,"):
		atlasLog, err = a.parseError(parts[2])
	case strings.HasPrefix(parts[2], "opuse,"):
		atlasLog, err = a.parseOpuse(parts[2])
	case strings.HasPrefix(parts[2], "hostload,"):
		atlasLog, err = a.parseHostload(parts[2])
	default:
		atlasLog = a.parseUnparsed(parts[2])
	}

	if err != nil {
		atlasLog = a.parseUnparsed(parts[2])
	}

	ts, err := time.Parse("2006 Jan 2 15:04:05", fmt.Sprintf("%d %s", time.Now().Year(),
		tsMsg))

	// adjust injected year if ts is close to new year
	if err != nil {
		return nil, err
	}

	atlasLog.Timestamp = ts
	atlasLog.Hostname = string(parts[0])

	return json.Marshal(atlasLog)
}

func (a *Atlas) parseResponse(msg string) (response *AtlasLog, err error) {
	// header|message1|message2|...
	parts := strings.Split(msg, "|")

	if len(parts) < 2 {
		return nil, fmt.Errorf("No messages found in: `%s`", msg)
	}

	// response,10:51:02.620,363776339/192.168.48.45/52401/414,00000BDF
	// command type, log_time, message_id/client_app_ip/client_app/port/socket_id, lenght_hex
	responseHeader := strings.Split(parts[0], ",")

	c, err := newClientHeader(responseHeader[2])
	if err != nil {
		return nil, err
	}

	r := &AtlasReqResp{
		LogTime:   responseHeader[1],
		LengthHex: responseHeader[3],
		Client:    c,
	}

	response = &AtlasLog{
		CommandType: responseHeader[0],
	}

	if responseHeader[0] == "request" {
		response.Command.Request = r
	} else {
		response.Command.Response = r
	}

	if parts[1][0] == '{' {
		unwrapped, err := unwrap(parts[1])
		if err != nil {
			return nil, err
		}
		r.Message = unwrapped.(map[string]interface{})
	} else {
		r.Message = make(map[string]interface{})
		for i, msgJSON := range parts[1:] {
			var unwrapped interface{}
			err = uhmarshal(msgJSON, &unwrapped)
			if err != nil {
				return nil, err
			}
			var cmd string
			if i == 0 {
				cmd = "header"
			} else {
				cmd = fmt.Sprintf("command%d", i)
			}
			r.Message[cmd] = unwrapped
		}
	}

	return
}

func (a *Atlas) parseError(msg string) (response *AtlasLog, err error) {
	// header|message1|message2|...
	parts := strings.Split(msg, "|")

	if len(parts) < 2 {
		return nil, fmt.Errorf("No messages found in: `%s`", msg)
	}

	headerParts := strings.Split(parts[0], ",")
	if len(headerParts) != 3 {
		return nil, fmt.Errorf("Invalid number of error header parts")
	}

	er := &AtlasError{}
	response = &AtlasLog{
		Command: AtlasCommand{Error: er},
	}

	response.CommandType = headerParts[0]
	er.LogTime = headerParts[1]
	er.LengthHex = headerParts[2]

	er.Message = make(map[string]interface{})
	for _, msgJSON := range parts[1:] {
		unwrapped, err := unwrap(msgJSON)
		if err != nil {
			return nil, err
		}
		for k, v := range unwrapped.(map[string]interface{}) {
			er.Message[k] = v
		}
	}

	return
}

func (a *Atlas) parseHostload(msg string) (response *AtlasLog, err error) {
	// hostload,13:24:05.630,ARZ,0,13:24:04,29
	parts := strings.Split(msg, ",")

	if len(parts) != 6 {
		return nil, fmt.Errorf("Invalid number of parts in `%s`", msg)
	}

	hl := &AtlasHostLoad{}
	response = &AtlasLog{
		CommandType: "hostload",
		Command:     AtlasCommand{Hostload: hl},
	}

	hl.LogTime = parts[1]
	hl.Vax = parts[2]
	hl.Load, err = strconv.Atoi(parts[3])
	if err != nil {
		return
	}
	hl.VaxTime = parts[4]
	hl.Flags, err = strconv.Atoi(parts[5])
	if err != nil {
		return
	}

	return
}

func (a *Atlas) parseOpuse(msg string) (response *AtlasLog, err error) {
	// opuse,14:54:35.450,14:54:35\tCH6\\6\\CartOps\\0\\0\\10\\8\\406347\\406361\\0\\100\\9
	parts := strings.Split(msg, "\\t")

	if len(parts) < 2 {
		return nil, fmt.Errorf("No messages found in: `%s`", msg)
	}

	headerParts := strings.Split(parts[0], ",")
	if len(headerParts) != 3 {
		return nil, fmt.Errorf("Invalid number of error header parts")
	}

	op := &AtlasOpUse{}
	response = &AtlasLog{
		CommandType: "opuse",
		Command:     AtlasCommand{Opuse: op},
	}

	response.CommandType = headerParts[0]

	op.LogTime = headerParts[1]
	op.VaxTime = headerParts[2]

	fields := strings.Split(parts[1], "\\")
	if len(fields) != 23 {
		return nil, fmt.Errorf("Invalid amount of fields in `%s`", parts[1])
	}

	op.Host = fields[0]
	op.PortSet, err = strconv.Atoi(fields[2])
	if err != nil {
		return
	}
	op.Usage = fields[4]
	op.UsedCur, err = strconv.Atoi(fields[6])
	if err != nil {
		return
	}
	op.QueCur, err = strconv.Atoi(fields[8])
	if err != nil {
		return
	}
	op.UsedPeak, err = strconv.Atoi(fields[10])
	if err != nil {
		return
	}
	op.QuePeak, err = strconv.Atoi(fields[12])
	if err != nil {
		return
	}
	op.UsedTot, err = strconv.Atoi(fields[14])
	if err != nil {
		return
	}
	op.QueTot, err = strconv.Atoi(fields[16])
	if err != nil {
		return
	}
	op.Min, err = strconv.Atoi(fields[18])
	if err != nil {
		return
	}
	op.Max, err = strconv.Atoi(fields[20])
	if err != nil {
		return
	}
	op.Ideal, err = strconv.Atoi(fields[22])
	if err != nil {
		return
	}
	response.Command.Opuse = op

	return
}

func (a *Atlas) parseUnparsed(msg string) (response *AtlasLog) {
	parts := strings.Split(msg, ",")
	response = &AtlasLog{
		CommandType: parts[0],
		Command:     AtlasCommand{Unparsed: msg},
	}
	return
}

func unwrap(msgJSON string) (m interface{}, err error) {
	type wrapper struct {
		Data string
	}
	w := &wrapper{}

	msgJSON = strings.Replace(msgJSON, "CITY-S", "CITY_S", -1)
	msgJSON = strings.Replace(msgJSON, "LINE#", "LINENUM", -1)
	fml := "{\"data\":\"" + msgJSON + "\"}"
	err = json.Unmarshal([]byte(fml), &w)
	if err == nil {
		err = json.Unmarshal([]byte(w.Data), &m)
	}
	if err != nil {
		err = uhmarshal(msgJSON, &m)
	}
	return
}

func newClientHeader(header string) (c *AtlasClientHeader, err error) {
	// 363776339/192.168.48.45/52401/414
	// message_id/client_app_ip/client_app/port/socket_id
	parts := strings.Split(header, "/")
	if len(parts) != 4 {
		return nil, fmt.Errorf("Invalid number of parts in '%s'", header)
	}

	message, err := strconv.Atoi(parts[0])
	if err != nil {
		return
	}
	ip := parts[1]
	port, err := strconv.Atoi(parts[2])
	if err != nil {
		return
	}
	socket, err := strconv.Atoi(parts[3])
	if err != nil {
		return
	}

	return &AtlasClientHeader{
		Message: message,
		IP:      ip,
		Port:    port,
		Socket:  socket,
	}, nil
}

func uhmarshal(m string, v interface{}) error {
	output := make(map[string]interface{})
	*v.(*interface{}) = output

	for _, p := range strings.FieldsFunc(m, func(c rune) bool { return c == ',' }) {
		parts := strings.Split(p, "=")
		if len(parts) != 2 {
			return fmt.Errorf("Invalid k=v: %s", p)
		}

		if v, err := strconv.Atoi(string(parts[1])); err == nil {
			output[string(parts[0])] = v
		} else {
			output[string(parts[0])] = string(parts[1])
		}
	}

	return nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (a *Atlas) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (a *Atlas) WaitForClose(timeout time.Duration) error {
	return nil
}
