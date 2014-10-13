package forwarder

import (
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfuse/signalfxproxy/listener"
	"testing"
	"github.com/cep21/gohelpers/a"
	"github.com/signalfuse/signalfxproxy/core/value"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/core"
	"time"
)

func TestSignalfxJSONForwarderLoader(t *testing.T) {
	listenFrom := config.ListenFrom{}
	listenFrom.ListenAddr = workarounds.GolangDoesnotAllowPointerToStringLiteral("0.0.0.0:12345")

	forwardTo := config.ForwardTo{
		URL: workarounds.GolangDoesnotAllowPointerToStringLiteral("http://0.0.0.0:12345/v1/datapoint"),
		MetricRegistrationURL: workarounds.GolangDoesnotAllowPointerToStringLiteral("http://0.0.0.0:12345/metrics"),
		DefaultAuthToken: workarounds.GolangDoesnotAllowPointerToStringLiteral("AUTH_TOKEN"),
		DefaultSource: workarounds.GolangDoesnotAllowPointerToStringLiteral("proxy-source"),
	}

	finalDatapointDestination := newBasicBufferedForwarder(100, 1, "", 1)
	l, err := listener.SignalFxListenerLoader(finalDatapointDestination, &listenFrom)
	defer l.Close()
	a.ExpectEquals(t, nil, err, "Expect no error")

	forwarder, err := SignalfxJSONForwarderLoader(&forwardTo)
	a.ExpectEquals(t, nil, err, "Expect no error")
	a.ExpectEquals(t, "signalfx-forwarder", forwarder.Name(), "Expect no error")
	a.ExpectEquals(t, 0, len(forwarder.GetStats()), "Expect no stats")
	timeToSend := time.Now().Round(time.Second)
	dpSent := core.NewAbsoluteTimeDatapoint("metric", map[string]string{}, value.NewIntWire(2), com_signalfuse_metrics_protobuf.MetricType_GAUGE, timeToSend)
	forwarder.DatapointsChannel() <- dpSent
	dpRecieved := <- finalDatapointDestination.datapointsChannel
	a.ExpectEquals(t, value.NewIntWire(2).WireValue(), dpRecieved.Value().WireValue(), "Expect 2 back")
}
