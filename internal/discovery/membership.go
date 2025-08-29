package discovery

import (
	"net"
	"os"

	"github.com/hashicorp/serf/serf"
	"github.com/rs/zerolog"
)

type Config struct {
	NodeName       string
	BindAddr       string
	Tags           map[string]string
	StartJoinAddrs []string
}

// Handler represents some component in our service that needs to know when a server joins or leaves the cluster.
type Handler interface {
	Join(name, add string) error
	Leave(name string) error
}

// Membership is our type wrapping Serf to provide discovery and cluster membership to our service.
type Membership struct {
	Config
	handler Handler
	serf    *serf.Serf
	events  chan serf.Event
	logger  *zerolog.Logger
}

func New(handler Handler, config Config) (*Membership, error) {
	logger := zerolog.New(os.Stderr).With().Str("service", "membership").Logger()
	c := &Membership{
		Config:  config,
		handler: handler,
		logger:  &logger,
	}

	if err := c.setupSerf(); err != nil {
		return nil, err
	}
	return c, nil
}

// setupSerf creates and configures a serf instance and starts the eventsHandler()
// goroutine to handle serf's events.
func (m *Membership) setupSerf() (err error) {
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return err
	}
	eventCh := make(chan serf.Event)
	m.events = eventCh

	config := serf.DefaultConfig()
	config.Init()
	// serf listens on this address and port for gossiping
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port
	// the event channel is how you'll receive Serf's events when a node joins or leaves the cluster
	// if you want a snapshot of the members at any point in time, you can call Serf's Members() method
	config.EventCh = eventCh
	// serf shares these tags to the other nodes in the cluster
	// and should use these tags for simple data that informs the cluster how to handle
	// this node. e.g. Consul shares each node's RPC address with Serf tags
	config.Tags = m.Tags
	config.NodeName = m.Config.NodeName
	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}

	go m.eventHandler()
	// when you have an existing cluster and you create a new node that you want to add to that cluster,
	// you need to point your new node to at least one of the nodes now in the cluster, it'll then
	// learn about the rest of the nodes in the existing cluster
	if m.StartJoinAddrs != nil {
		// you set the field to the addresses of nodes in the cluster, and serf's gossip protocol takes
		// care of the rest to join your node to the cluster.
		// In a production environment, specify at least three addresses to make your cluster resilient
		// to one or two node failures or a disrupted network.
		_, err := m.serf.Join(m.StartJoinAddrs, true)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Membership) eventHandler() {
	// reading events sent by Serf into the events channel
	// when a node joins or leaves the cluster, Serf sends an event to all nodes,
	// including the node that joined or left the cluster.
	for e := range m.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				m.handleJoin(member)
			}
		case serf.EventMemberLeave:
			for _, member := range e.(serf.MemberEvent).Members {
				// we check whether the node we got an event for is the local server
				// so that the server doesn't act on itself
				if m.isLocal(member) {
					return
				}
				m.handleLeave(member)
			}
		}
	}
}

func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

func (m *Membership) Leave() error {
	return m.serf.Leave()
}

func (m *Membership) logError(err error, msg string, member serf.Member) {
	m.logger.Error().Err(err).Str("name", member.Name).Str("rpc_addr", member.Tags["rpc_addr"]).Msg(msg)
}

func (m *Membership) handleJoin(member serf.Member) {
	if err := m.handler.Join(member.Name, member.Tags["rpc_addr"]); err != nil {
		m.logError(err, "failed to join", member)
	} else {
		m.logger.Info().Str("name", member.Name).Str("event", "join").Msg("member joined")
	}
}

func (m *Membership) handleLeave(member serf.Member) {
	if err := m.handler.Leave(member.Name); err != nil {
		m.logError(err, "failed to leave", member)
	} else {
		m.logger.Info().Str("name", member.Name).Str("event", "leave").Msg("member left")
	}
}
