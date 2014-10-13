package gangliaxdr

import (
	"errors"
	"github.com/davecgh/go-xdr/xdr"
	"net"
	"strconv"
)

// HandlerFunc handles ganglia input
type HandlerFunc func(*GangliaInput)

// GangliaInput is the input datapoint for ganglia
type GangliaInput struct {
	Version    int32
	Hostname   string
	MetricName string
	Spoofed    bool
	Value      DatapointMaker
}

type gangliaMetadata struct {
	Version     int32
	Hostname    string
	Metric      string
	Spoofed     bool
	Type        string
	MetricName2 string
	Units       string
	Slope       int32
	Tmax        int32
	Dmax        int32
	Data        map[string]string
}

// DatapointMaker creates the value for the datapoint from the ganglia input
type DatapointMaker interface {
}

type gangliaFloatValue struct {
	Value float64
}

type gangliaIntValue struct {
	Value int64
}

type gangliaStringValue struct {
	Value string
}

// CreateValue creates a datapoint maker from raw ganglia datapoint bytes
func CreateValue(format string, buf []byte, metadata *gangliaMetadata) (dp DatapointMaker, err error) {
	switch format[len(format)-1:] {
	case "f":
		var val gangliaFloatValue
		if buf, err = xdr.Unmarshal(buf, &val.Value); err == nil {
			dp = val
		}
		return
	case "u", "d":
		var val gangliaIntValue
		if buf, err = xdr.Unmarshal(buf, &val.Value); err == nil {
			dp = val
		}
		return
	case "s":
		var val gangliaStringValue
		if buf, err = xdr.Unmarshal(buf, &val.Value); err != nil {
			return
		}
		dp = val
		if metadata != nil {
			switch metadata.Type {
			case "float", "double":
				var val2 gangliaFloatValue
				var err2 error
				if val2.Value, err2 = strconv.ParseFloat(val.Value, 64); err2 == nil {
					dp = val2
				}
			case "int8", "uint8", "int16", "uint16", "int32", "uint32":
				var val2 gangliaIntValue
				var err2 error
				if val2.Value, err2 = strconv.ParseInt(val.Value, 10, 64); err2 == nil {
					dp = val2
				}
			}
		}
		return
	}

	err = errors.New("Invalid format " + format)
	return
}

// createInput parses the raw ganglia byte to find out when we need to create a ganglia point
func createInput(buf []byte, givenMetadata *gangliaMetadata) (input *GangliaInput, metadata *gangliaMetadata, err error) {
	var version int32
	if buf, err = xdr.Unmarshal(buf, &version); err != nil {
		return
	}
	if version == 128 {
		var gmetadata gangliaMetadata
		gmetadata, err = readGangliaMetadata(buf)
		metadata = &gmetadata
		return
	}
	var H GangliaInput
	input = &H
	input.Version = version
	if input.Version != 132 && input.Version != 133 && input.Version != 134 {
		return
	}
	if buf, err = xdr.Unmarshal(buf, &input.Hostname); err != nil {
		return
	}
	if buf, err = xdr.Unmarshal(buf, &input.MetricName); err != nil {
		return
	}
	var spoofInt int32
	if buf, err = xdr.Unmarshal(buf, &spoofInt); err != nil {
		return
	}
	input.Spoofed = (spoofInt != 0)
	if input.Spoofed && len(input.Hostname) > 0 && input.Hostname[0] == ':' {
		input.Hostname = input.Hostname[1:]
	}
	var format string
	if buf, err = xdr.Unmarshal(buf, &format); err != nil {
		return
	}
	input.Value, err = CreateValue(format, buf, givenMetadata)
	return
}

// readGangliaMetadata reads a metadata request from the current ganglia input stream
func readGangliaMetadata(buf []byte) (gmeta gangliaMetadata, err error) {
	gmeta.Version = 128
	if buf, err = xdr.Unmarshal(buf, &gmeta.Hostname); err != nil {
		return
	}
	if buf, err = xdr.Unmarshal(buf, &gmeta.Metric); err != nil {
		return
	}
	var spoofInt int32
	if buf, err = xdr.Unmarshal(buf, &spoofInt); err != nil {
		return
	}
	gmeta.Spoofed = (spoofInt != 0)

	if buf, err = xdr.Unmarshal(buf, &gmeta.Type); err != nil {
		return
	}

	if buf, err = xdr.Unmarshal(buf, &gmeta.MetricName2); err != nil {
		return
	}

	if buf, err = xdr.Unmarshal(buf, &gmeta.Units); err != nil {
		return
	}

	if buf, err = xdr.Unmarshal(buf, &gmeta.Slope); err != nil {
		return
	}

	if buf, err = xdr.Unmarshal(buf, &gmeta.Tmax); err != nil {
		return
	}

	if buf, err = xdr.Unmarshal(buf, &gmeta.Dmax); err != nil {
		return
	}

	var remaining int32
	if buf, err = xdr.Unmarshal(buf, &remaining); err != nil {
		return
	}

	gmeta.Data = make(map[string]string)

	for remaining > 0 {
		remaining--
		var key string
		var value string
		if buf, err = xdr.Unmarshal(buf, &key); err != nil {
			return
		}
		if buf, err = xdr.Unmarshal(buf, &value); err != nil {
			return
		}
		gmeta.Data[key] = value
	}
	return
}

// SocketListen opens a UDP socket to read and send ganglia data
func SocketListen(sock *net.UDPConn, f HandlerFunc, bufferSize int) {
	var err error
	var meta *gangliaMetadata
	var buf []byte
	buf = make([]byte, bufferSize)
	for {
		var input *GangliaInput
		var rlen int
		reset := (meta != nil)

		if rlen, _, err = sock.ReadFromUDP(buf); err != nil {
			return
		}
		input, meta, err = createInput(buf[:rlen], meta)
		if reset {
			// Do we keep metadata?  Not sure
			meta = nil
		}
		if input != nil {
			f(input)
		}
	}
}
