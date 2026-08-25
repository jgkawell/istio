// Copyright Istio Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"context"
	"net"
	"sync/atomic"
	"time"

	"codeberg.org/miekg/dns"
)

type dnsProxy struct {
	server *dns.Server

	// This is the upstream Client used to make upstream DNS queries
	// in case the data is not in our name table.
	upstreamClient *dns.Client
	protocol       string
	resolver       *LocalDNSServer
	// started tracks whether the server is actually listening (set from NotifyStartedFunc).
	// In v2, dns.Server.Shutdown dereferences state set up by ListenAndServe and panics if
	// the server was never started, so close() must only shut down a started server.
	started atomic.Bool
}

func newDNSProxy(protocol, addr string, resolver *LocalDNSServer, timeout time.Duration) (*dnsProxy, error) {
	p := &dnsProxy{
		server: &dns.Server{},
		// The network (udp/tcp) is passed per-Exchange in v2; timeouts live on the Transport.
		upstreamClient: &dns.Client{
			Transport: &dns.Transport{
				Dialer:       &net.Dialer{Timeout: timeout},
				ReadTimeout:  timeout,
				WriteTimeout: timeout,
			},
		},
		protocol: protocol,
		resolver: resolver,
	}

	var err error
	// dnsProxy is itself the DNS handler for every query. v2's dns.ServeMux no longer
	// treats "." as a catch-all for arbitrary names, so set the Handler directly instead
	// of routing through a ServeMux.
	p.server.Handler = p
	// Mark the server started only once it is actually listening. ListenAndServe sets up
	// the state Shutdown dereferences (cancel func, shutdown/exited channels) inside its
	// init(); flipping the flag here (rather than at the top of start()) closes the window
	// where close() could call Shutdown before that state exists and panic.
	p.server.NotifyStartedFunc = func(context.Context) {
		p.started.Store(true)
	}
	if protocol == "udp" {
		p.server.PacketConn, err = net.ListenPacket("udp", addr)
	} else {
		p.server.Listener, err = net.Listen("tcp", addr)
	}
	log.Infof("Starting local %s DNS server on %v", p.protocol, addr)
	if err != nil {
		log.Errorf("Failed to listen on %s port %s: %v", protocol, addr, err)
		return nil, err
	}
	return p, nil
}

func (p *dnsProxy) start() {
	err := p.server.ListenAndServe()
	if err != nil {
		log.Errorf("Local %s DNS server terminated: %v", p.protocol, err)
	}
}

func (p *dnsProxy) close() {
	if p.server != nil && p.started.Load() {
		p.server.Shutdown(context.Background())
	}
}

func (p *dnsProxy) Address() string {
	if p.server != nil {
		if p.server.Listener != nil {
			return p.server.Listener.Addr().String()
		}
		if p.server.PacketConn != nil {
			return p.server.PacketConn.LocalAddr().String()
		}
	}
	return ""
}

func (p *dnsProxy) ServeDNS(ctx context.Context, w dns.ResponseWriter, req *dns.Msg) {
	p.resolver.ServeDNS(ctx, p, w, req)
}
