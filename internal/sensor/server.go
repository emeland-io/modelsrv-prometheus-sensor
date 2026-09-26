package sensor

import (
	"fmt"
	"net"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"go.emeland.io/modelsrv/pkg/client"
	"go.emeland.io/modelsrv/pkg/endpoint"
	"go.emeland.io/modelsrv/pkg/events"
	"go.emeland.io/modelsrv/pkg/model"
	"go.emeland.io/modelsrv/pkg/model/node"
)

// prometheusSensorNodeTypeID is the stable identity for the "prometheus-sensor"
// NodeType. All instances of this sensor share this UUID. It is a deterministic
// UUIDv5 (namespace emeland.io, name "prometheus-sensor.nodetype") and must not
// change after the first deployment.
var prometheusSensorNodeTypeID = uuid.MustParse("23a0f50e-3d81-599c-9462-e3c565b4547d")

// Server runs a modelsrv web endpoint backed by a local model. It receives
// Metric definitions pushed from an upstream node into that model and forwards
// emitted MetricValue events to downstream subscribers.
type Server struct {
	events *eventManager
	model  model.Model
	nodeID uuid.UUID
	log    *zap.SugaredLogger
}

// New starts a sensor server bound to listenAddr, pre-registering any provided
// downstream subscriber URLs.
func New(listenAddr string, subscribers []string, log *zap.SugaredLogger) (*Server, error) {
	if log == nil {
		log = zap.NewNop().Sugar()
	}

	em := newEventManager(log)

	sink, err := em.GetSink()
	if err != nil {
		return nil, err
	}

	m, err := model.NewModel(sink)
	if err != nil {
		return nil, err
	}

	nt := node.NewNodeType(prometheusSensorNodeTypeID)
	nt.SetDisplayName("prometheus-sensor")
	if err := m.AddNodeType(nt); err != nil {
		return nil, fmt.Errorf("register node type: %w", err)
	}

	nodeID := uuid.New()
	n := node.NewNode(nodeID)
	n.SetDisplayName("prometheus-sensor")
	n.SetNodeTypeByRef(nt)
	if err := m.AddNode(n); err != nil {
		return nil, fmt.Errorf("register node: %w", err)
	}

	// Register downstream subscribers only after NodeType + Node exist so
	// AddSubscriber's replay delivers them reliably.
	for _, s := range subscribers {
		if err := em.AddSubscriber(s); err != nil {
			return nil, err
		}
	}

	if err := endpoint.StartWebListener(m, em, listenAddr, endpoint.WebListenerOptions{Logger: log}); err != nil {
		return nil, err
	}

	log.Infow("sensor registered",
		"nodeTypeId", prometheusSensorNodeTypeID,
		"nodeId", nodeID,
	)
	log.Infow("subscriber management endpoints available",
		"register", fmt.Sprintf("http://%s/api/events/register", listenAddr),
		"unregister", fmt.Sprintf("http://%s/api/events/unregister", listenAddr),
		"subscribers", fmt.Sprintf("http://%s/api/events/subscribers", listenAddr),
	)

	return &Server{events: em, model: m, nodeID: nodeID, log: log}, nil
}

// RegisterUpstream subscribes this sensor to an upstream emeland node so the
// upstream pushes its Metric (and other) events to callbackURL. callbackURL is
// this sensor's own API base URL (e.g. "http://host:24200/api/").
func (s *Server) RegisterUpstream(upstreamURL, callbackURL string) error {
	c, err := client.NewModelSrvClient(upstreamURL)
	if err != nil {
		return fmt.Errorf("build upstream client for %q: %w", upstreamURL, err)
	}
	if err := c.Register(callbackURL); err != nil {
		return fmt.Errorf("register callback %q with upstream %q: %w", callbackURL, upstreamURL, err)
	}
	s.log.Infow("registered with upstream", "upstream", upstreamURL, "callback", callbackURL)
	return nil
}

// Model returns the local model backing this sensor. Callers use it to read
// the Metric definitions pushed in from upstream.
func (s *Server) Model() model.Model {
	return s.model
}

// Addr returns the address the sensor's web endpoint is listening on. This is
// useful when New was given a wildcard port (":0") to discover the bound port.
func (s *Server) Addr() net.Addr {
	return endpoint.WebListenerAddr()
}

// Close shuts down the sensor's HTTP listener and deregisters its Node.
func (s *Server) Close() error {
	if err := s.model.DeleteNodeById(s.nodeID); err != nil {
		s.log.Warnw("failed to delete node on shutdown", "nodeId", s.nodeID, "error", err)
	}
	endpoint.StopWebListener()
	return nil
}

// Emit applies the event to this process's model and forwards it through the
// model sink to the event manager (master recording + subscriber notify).
func (s *Server) Emit(ev events.Event) error {
	return s.model.Apply(ev)
}

// MasterEvents returns a snapshot of all events recorded in the master list.
func (s *Server) MasterEvents() []events.Event {
	if s == nil || s.events == nil {
		return nil
	}
	return s.events.snapshotMasterEvents()
}
