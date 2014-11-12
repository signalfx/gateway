package listener

import (
	"encoding/json"
	"github.com/cep21/gohelpers/structdefaults"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/golang/glog"
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/core"
	"github.com/signalfuse/signalfxproxy/protocoltypes"
	"net"
	"net/http"
	"time"
)

type collectdListenerServer struct {
	listener              net.Listener
	server                *http.Server
	datapointStreamingAPI core.DatapointStreamingAPI
}

func (streamer *collectdListenerServer) GetStats() []core.Datapoint {
	ret := []core.Datapoint{}
	return ret
}

func (streamer *collectdListenerServer) Close() {
	streamer.listener.Close()
}

func (streamer *collectdListenerServer) jsonDecode(req *http.Request) error {
	dec := json.NewDecoder(req.Body)
	var d protocoltypes.CollectdJSONWriteBody
	if err := dec.Decode(&d); err != nil {
		return err
	}
	for _, f := range d {
		if f.TypeS != nil && f.Time != nil {
			for i := range f.Dsnames {
				if i < len(f.Dstypes) && i < len(f.Values) {
					streamer.datapointStreamingAPI.DatapointsChannel() <- protocoltypes.NewCollectdDatapoint(f, uint(i))
				}
			}
		}
	}
	return nil
}

func (streamer *collectdListenerServer) handleCollectd(writter http.ResponseWriter, req *http.Request) {
	knownTypes := map[string]func(*http.Request) error{
		"application/json": streamer.jsonDecode,
	}
	contentType := req.Header.Get("Content-type")
	decoderFunc, ok := knownTypes[contentType]

	if !ok {
		writter.WriteHeader(http.StatusBadRequest)
		writter.Write([]byte(`{msg:"Unknown content type"}`))
		return
	}
	err := decoderFunc(req)
	if err != nil {
		writter.WriteHeader(http.StatusBadRequest)
		writter.Write([]byte(err.Error()))
	} else {
		writter.WriteHeader(http.StatusOK)
	}
}

var defaultCollectdConfig = &config.ListenFrom{
	ListenAddr:      workarounds.GolangDoesnotAllowPointerToStringLiteral("0.0.0.0:8081"),
	TimeoutDuration: workarounds.GolangDoesnotAllowPointerToTimeLiteral(time.Second * 30),
	ListenPath:      workarounds.GolangDoesnotAllowPointerToStringLiteral("/post-collectd"),
}

// CollectdListenerLoader loads a listener for collectd write_http protocol
func CollectdListenerLoader(DatapointStreamingAPI core.DatapointStreamingAPI, listenFrom *config.ListenFrom) (DatapointListener, error) {
	structdefaults.FillDefaultFrom(listenFrom, defaultCollectdConfig)
	glog.Infof("Creating signalfx listener using final config %s", listenFrom)
	return StartListeningCollectDHTTPOnPort(DatapointStreamingAPI, *listenFrom.ListenAddr, *listenFrom.ListenPath, *listenFrom.TimeoutDuration)
}

// StartListeningCollectDHTTPOnPort servers http collectd requests
func StartListeningCollectDHTTPOnPort(DatapointStreamingAPI core.DatapointStreamingAPI, listenAddr string, listenPath string, clientTimeout time.Duration) (DatapointListener, error) {
	mux := http.NewServeMux()

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	server := &http.Server{
		Handler:      mux,
		Addr:         listenAddr,
		ReadTimeout:  clientTimeout,
		WriteTimeout: clientTimeout,
	}

	listenServer := collectdListenerServer{
		listener:              listener,
		server:                server,
		datapointStreamingAPI: DatapointStreamingAPI,
	}

	mux.HandleFunc(
		listenPath,
		listenServer.handleCollectd)

	go server.Serve(listener)
	return &listenServer, err
}
