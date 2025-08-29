package discovery_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	. "github.com/ttaaoo/proglog/internal/discovery"
)

// Sets up a cluster with multiple servers and checks that the Membership
// returns all the servers that joined the membership and updates after a server leaves the cluster.
func TestMembership(t *testing.T) {
	m, handler := setupMember(t, nil)
	m, _ = setupMember(t, m)
	m, _ = setupMember(t, m)

	require.Eventually(t, func() bool {
		return 2 == len(handler.joins) &&
			3 == len(m[0].Members()) &&
			0 == len(handler.leaves)
	}, 3*time.Second, 250*time.Millisecond)
	require.NoError(t, m[2].Leave())

	require.Eventually(t, func() bool {
		return 2 == len(handler.joins) &&
			3 == len(m[0].Members()) &&
			serf.StatusLeft == m[0].Members()[2].Status &&
			1 == len(handler.leaves)
	}, 3*time.Second, 250*time.Millisecond)
	require.Equal(t, fmt.Sprintf("%d", 2), <-handler.leaves)
}

// the handler mock tracks how many times the Join and Leave methods are called
// and the values passed to them.
type handler struct {
	joins  chan map[string]string
	leaves chan string
}

func (h *handler) Join(id, addr string) error {
	if h.joins != nil {
		h.joins <- map[string]string{
			"id":   id,
			"addr": addr,
		}
	}
	return nil
}

func (h *handler) Leave(name string) error {
	if h.leaves != nil {
		h.leaves <- name
	}
	return nil
}

// The member's length also tells us whether this member is the cluster's
// initial member or we have a cluster to join.
func setupMember(t *testing.T, members []*Membership) ([]*Membership, *handler) {
	id := len(members)
	ports := dynaport.Get(1)
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
	tags := map[string]string{
		"rpc_addr": addr,
	}
	c := Config{
		NodeName: fmt.Sprintf("%d", id),
		BindAddr: addr,
		Tags:     tags,
	}
	h := &handler{}
	if len(members) == 0 {
		// initial member of the cluster
		h.joins = make(chan map[string]string, 3)
		h.leaves = make(chan string, 3)
	} else {
		// join a cluster
		c.StartJoinAddrs = []string{members[0].BindAddr}
	}

	m, err := New(h, c)
	require.NoError(t, err)
	members = append(members, m)

	return members, h
}
