package shadowtls

import (
	"context"
	"net"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/common/dialer"
	"github.com/sagernet/sing-box/common/listener"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-shadowtls"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/auth"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func RegisterInbound(registry *inbound.Registry) {
	inbound.Register[option.ShadowTLSInboundOptions](registry, C.TypeShadowTLS, NewInbound)
}

type Inbound struct {
	inbound.Adapter
	router   adapter.Router
	logger   logger.ContextLogger
	listener *listener.Listener
	service  *shadowtls.Service
	tracker  adapter.SSMTracker
	version  int
}

var _ adapter.ManagedSSMServer = (*Inbound)(nil)

func NewInbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.ShadowTLSInboundOptions) (adapter.Inbound, error) {
	inbound := &Inbound{
		Adapter: inbound.NewAdapter(C.TypeShadowTLS, tag),
		router:  router,
		logger:  logger,
		version: options.Version,
	}

	if options.Version == 0 {
		options.Version = 1
	}

	var handshakeForServerName map[string]shadowtls.HandshakeConfig
	if options.Version > 1 {
		handshakeForServerName = make(map[string]shadowtls.HandshakeConfig)
		if options.HandshakeForServerName != nil {
			for _, entry := range options.HandshakeForServerName.Entries() {
				handshakeDialer, err := dialer.New(ctx, entry.Value.DialerOptions, entry.Value.ServerIsDomain())
				if err != nil {
					return nil, err
				}
				handshakeForServerName[entry.Key] = shadowtls.HandshakeConfig{
					Server: entry.Value.ServerOptions.Build(),
					Dialer: handshakeDialer,
				}
			}
		}
	}
	serverIsDomain := options.Handshake.ServerIsDomain()
	if options.WildcardSNI != option.ShadowTLSWildcardSNIOff {
		serverIsDomain = true
	}
	handshakeDialer, err := dialer.New(ctx, options.Handshake.DialerOptions, serverIsDomain)
	if err != nil {
		return nil, err
	}
	service, err := shadowtls.NewService(shadowtls.ServiceConfig{
		Version:  options.Version,
		Password: options.Password,
		Users: common.Map(options.Users, func(it option.ShadowTLSUser) shadowtls.User {
			return (shadowtls.User)(it)
		}),
		Handshake: shadowtls.HandshakeConfig{
			Server: options.Handshake.ServerOptions.Build(),
			Dialer: handshakeDialer,
		},
		HandshakeForServerName: handshakeForServerName,
		StrictMode:             options.StrictMode,
		WildcardSNI:            shadowtls.WildcardSNI(options.WildcardSNI),
		Handler:                (*inboundHandler)(inbound),
		Logger:                 logger,
	})
	if err != nil {
		return nil, err
	}
	inbound.service = service
	inbound.listener = listener.New(listener.Options{
		Context:           ctx,
		Logger:            logger,
		Network:           []string{N.NetworkTCP},
		Listen:            options.ListenOptions,
		ConnectionHandler: inbound,
	})
	return inbound, nil
}

func (h *Inbound) Start(stage adapter.StartStage) error {
	if stage != adapter.StartStateStart {
		return nil
	}
	return h.listener.Start()
}

func (h *Inbound) Close() error {
	return h.listener.Close()
}

func (h *Inbound) SetTracker(tracker adapter.SSMTracker) {
	h.tracker = tracker
}

func (h *Inbound) UpdateUsers(users []string, uPSKs []string) error {
	if h.version < 3 {
		return nil
	}
	shadowTLSUsers := make([]shadowtls.User, 0, len(users))
	for i, user := range users {
		shadowTLSUsers = append(shadowTLSUsers, shadowtls.User{
			Name:     user,
			Password: uPSKs[i],
		})
	}
	h.service.UpdateUsers(shadowTLSUsers)
	return nil
}

func (h *Inbound) NewConnection(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	err := h.service.NewConnection(adapter.WithContext(log.ContextWithNewID(ctx), &metadata), conn, metadata.Source, metadata.Destination, onClose)
	N.CloseOnHandshakeFailure(conn, onClose, err)
	if err != nil {
		if E.IsClosedOrCanceled(err) {
			h.logger.DebugContext(ctx, "connection closed: ", err)
		} else {
			h.logger.ErrorContext(ctx, E.Cause(err, "process connection from ", metadata.Source))
		}
	}
}

type inboundHandler Inbound

func (h *inboundHandler) NewConnectionEx(ctx context.Context, conn net.Conn, source M.Socksaddr, destination M.Socksaddr, onClose N.CloseHandlerFunc) {
	var metadata adapter.InboundContext
	metadata.Inbound = h.Tag()
	metadata.InboundType = h.Type()
	//nolint:staticcheck
	metadata.InboundDetour = h.listener.ListenOptions().Detour
	//nolint:staticcheck
	metadata.Source = source
	metadata.Destination = destination
	if userName, _ := auth.UserFromContext[string](ctx); userName != "" {
		metadata.User = userName
		h.logger.InfoContext(ctx, "[", userName, "] inbound connection to ", metadata.Destination)
	} else {
		h.logger.InfoContext(ctx, "inbound connection to ", metadata.Destination)
	}
	if h.tracker != nil {
		conn = h.tracker.TrackConnection(conn, metadata)
	}
	h.router.RouteConnectionEx(ctx, conn, metadata, onClose)
}
