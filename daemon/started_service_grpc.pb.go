package daemon

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	StartedService_StopService_FullMethodName              = "/daemon.StartedService/StopService"
	StartedService_ReloadService_FullMethodName            = "/daemon.StartedService/ReloadService"
	StartedService_SubscribeServiceStatus_FullMethodName   = "/daemon.StartedService/SubscribeServiceStatus"
	StartedService_SubscribeLog_FullMethodName             = "/daemon.StartedService/SubscribeLog"
	StartedService_GetDefaultLogLevel_FullMethodName       = "/daemon.StartedService/GetDefaultLogLevel"
	StartedService_ClearLogs_FullMethodName                = "/daemon.StartedService/ClearLogs"
	StartedService_SubscribeStatus_FullMethodName          = "/daemon.StartedService/SubscribeStatus"
	StartedService_SubscribeGroups_FullMethodName          = "/daemon.StartedService/SubscribeGroups"
	StartedService_GetClashModeStatus_FullMethodName       = "/daemon.StartedService/GetClashModeStatus"
	StartedService_SubscribeClashMode_FullMethodName       = "/daemon.StartedService/SubscribeClashMode"
	StartedService_SetClashMode_FullMethodName             = "/daemon.StartedService/SetClashMode"
	StartedService_URLTest_FullMethodName                  = "/daemon.StartedService/URLTest"
	StartedService_SelectOutbound_FullMethodName           = "/daemon.StartedService/SelectOutbound"
	StartedService_SetGroupExpand_FullMethodName           = "/daemon.StartedService/SetGroupExpand"
	StartedService_GetSystemProxyStatus_FullMethodName     = "/daemon.StartedService/GetSystemProxyStatus"
	StartedService_SetSystemProxyEnabled_FullMethodName    = "/daemon.StartedService/SetSystemProxyEnabled"
	StartedService_TriggerDebugCrash_FullMethodName        = "/daemon.StartedService/TriggerDebugCrash"
	StartedService_TriggerOOMReport_FullMethodName         = "/daemon.StartedService/TriggerOOMReport"
	StartedService_SubscribeConnections_FullMethodName     = "/daemon.StartedService/SubscribeConnections"
	StartedService_CloseConnection_FullMethodName          = "/daemon.StartedService/CloseConnection"
	StartedService_CloseAllConnections_FullMethodName      = "/daemon.StartedService/CloseAllConnections"
	StartedService_GetDeprecatedWarnings_FullMethodName    = "/daemon.StartedService/GetDeprecatedWarnings"
	StartedService_GetStartedAt_FullMethodName             = "/daemon.StartedService/GetStartedAt"
	StartedService_SubscribeOutbounds_FullMethodName       = "/daemon.StartedService/SubscribeOutbounds"
	StartedService_StartNetworkQualityTest_FullMethodName  = "/daemon.StartedService/StartNetworkQualityTest"
	StartedService_StartSTUNTest_FullMethodName            = "/daemon.StartedService/StartSTUNTest"
	StartedService_SubscribeTailscaleStatus_FullMethodName = "/daemon.StartedService/SubscribeTailscaleStatus"
	StartedService_StartTailscalePing_FullMethodName       = "/daemon.StartedService/StartTailscalePing"
	StartedService_ListUsers_FullMethodName                = "/daemon.StartedService/ListUsers"
	StartedService_GetUser_FullMethodName                  = "/daemon.StartedService/GetUser"
	StartedService_AddUser_FullMethodName                  = "/daemon.StartedService/AddUser"
	StartedService_UpdateUser_FullMethodName               = "/daemon.StartedService/UpdateUser"
	StartedService_DeleteUser_FullMethodName               = "/daemon.StartedService/DeleteUser"
	StartedService_GetInboundStats_FullMethodName          = "/daemon.StartedService/GetInboundStats"
	StartedService_ListInbounds_FullMethodName             = "/daemon.StartedService/ListInbounds"
	StartedService_AddInbound_FullMethodName               = "/daemon.StartedService/AddInbound"
	StartedService_RemoveInbound_FullMethodName            = "/daemon.StartedService/RemoveInbound"
)

// StartedServiceClient is the client API for StartedService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type StartedServiceClient interface {
	StopService(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*emptypb.Empty, error)
	ReloadService(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*emptypb.Empty, error)
	SubscribeServiceStatus(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (grpc.ServerStreamingClient[ServiceStatus], error)
	SubscribeLog(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (grpc.ServerStreamingClient[Log], error)
	GetDefaultLogLevel(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*DefaultLogLevel, error)
	ClearLogs(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*emptypb.Empty, error)
	SubscribeStatus(ctx context.Context, in *SubscribeStatusRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[Status], error)
	SubscribeGroups(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (grpc.ServerStreamingClient[Groups], error)
	GetClashModeStatus(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*ClashModeStatus, error)
	SubscribeClashMode(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (grpc.ServerStreamingClient[ClashMode], error)
	SetClashMode(ctx context.Context, in *ClashMode, opts ...grpc.CallOption) (*emptypb.Empty, error)
	URLTest(ctx context.Context, in *URLTestRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	SelectOutbound(ctx context.Context, in *SelectOutboundRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	SetGroupExpand(ctx context.Context, in *SetGroupExpandRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	GetSystemProxyStatus(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*SystemProxyStatus, error)
	SetSystemProxyEnabled(ctx context.Context, in *SetSystemProxyEnabledRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	TriggerDebugCrash(ctx context.Context, in *DebugCrashRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	TriggerOOMReport(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*emptypb.Empty, error)
	SubscribeConnections(ctx context.Context, in *SubscribeConnectionsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[ConnectionEvents], error)
	CloseConnection(ctx context.Context, in *CloseConnectionRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	CloseAllConnections(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*emptypb.Empty, error)
	GetDeprecatedWarnings(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*DeprecatedWarnings, error)
	GetStartedAt(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*StartedAt, error)
	SubscribeOutbounds(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (grpc.ServerStreamingClient[OutboundList], error)
	StartNetworkQualityTest(ctx context.Context, in *NetworkQualityTestRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[NetworkQualityTestProgress], error)
	StartSTUNTest(ctx context.Context, in *STUNTestRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[STUNTestProgress], error)
	SubscribeTailscaleStatus(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (grpc.ServerStreamingClient[TailscaleStatusUpdate], error)
	StartTailscalePing(ctx context.Context, in *TailscalePingRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[TailscalePingResponse], error)
	// User management
	ListUsers(ctx context.Context, in *ListUsersRequest, opts ...grpc.CallOption) (*UserList, error)
	GetUser(ctx context.Context, in *GetUserRequest, opts ...grpc.CallOption) (*UserInfo, error)
	AddUser(ctx context.Context, in *AddUserRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	UpdateUser(ctx context.Context, in *UpdateUserRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	DeleteUser(ctx context.Context, in *DeleteUserRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	// Traffic statistics
	GetInboundStats(ctx context.Context, in *GetInboundStatsRequest, opts ...grpc.CallOption) (*InboundStats, error)
	// Inbound management
	ListInbounds(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*InboundList, error)
	AddInbound(ctx context.Context, in *AddInboundRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	RemoveInbound(ctx context.Context, in *RemoveInboundRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
}

type startedServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewStartedServiceClient(cc grpc.ClientConnInterface) StartedServiceClient {
	return &startedServiceClient{cc}
}

func (c *startedServiceClient) StopService(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, StartedService_StopService_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *startedServiceClient) ReloadService(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, StartedService_ReloadService_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *startedServiceClient) SubscribeServiceStatus(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (grpc.ServerStreamingClient[ServiceStatus], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &StartedService_ServiceDesc.Streams[0], StartedService_SubscribeServiceStatus_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[emptypb.Empty, ServiceStatus]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type StartedService_SubscribeServiceStatusClient = grpc.ServerStreamingClient[ServiceStatus]

func (c *startedServiceClient) SubscribeLog(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (grpc.ServerStreamingClient[Log], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &StartedService_ServiceDesc.Streams[1], StartedService_SubscribeLog_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[emptypb.Empty, Log]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type StartedService_SubscribeLogClient = grpc.ServerStreamingClient[Log]

func (c *startedServiceClient) GetDefaultLogLevel(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*DefaultLogLevel, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(DefaultLogLevel)
	err := c.cc.Invoke(ctx, StartedService_GetDefaultLogLevel_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *startedServiceClient) ClearLogs(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, StartedService_ClearLogs_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *startedServiceClient) SubscribeStatus(ctx context.Context, in *SubscribeStatusRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[Status], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &StartedService_ServiceDesc.Streams[2], StartedService_SubscribeStatus_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[SubscribeStatusRequest, Status]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type StartedService_SubscribeStatusClient = grpc.ServerStreamingClient[Status]

func (c *startedServiceClient) SubscribeGroups(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (grpc.ServerStreamingClient[Groups], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &StartedService_ServiceDesc.Streams[3], StartedService_SubscribeGroups_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[emptypb.Empty, Groups]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type StartedService_SubscribeGroupsClient = grpc.ServerStreamingClient[Groups]

func (c *startedServiceClient) GetClashModeStatus(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*ClashModeStatus, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ClashModeStatus)
	err := c.cc.Invoke(ctx, StartedService_GetClashModeStatus_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *startedServiceClient) SubscribeClashMode(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (grpc.ServerStreamingClient[ClashMode], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &StartedService_ServiceDesc.Streams[4], StartedService_SubscribeClashMode_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[emptypb.Empty, ClashMode]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type StartedService_SubscribeClashModeClient = grpc.ServerStreamingClient[ClashMode]

func (c *startedServiceClient) SetClashMode(ctx context.Context, in *ClashMode, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, StartedService_SetClashMode_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *startedServiceClient) URLTest(ctx context.Context, in *URLTestRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, StartedService_URLTest_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *startedServiceClient) SelectOutbound(ctx context.Context, in *SelectOutboundRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, StartedService_SelectOutbound_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *startedServiceClient) SetGroupExpand(ctx context.Context, in *SetGroupExpandRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, StartedService_SetGroupExpand_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *startedServiceClient) GetSystemProxyStatus(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*SystemProxyStatus, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(SystemProxyStatus)
	err := c.cc.Invoke(ctx, StartedService_GetSystemProxyStatus_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *startedServiceClient) SetSystemProxyEnabled(ctx context.Context, in *SetSystemProxyEnabledRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, StartedService_SetSystemProxyEnabled_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *startedServiceClient) TriggerDebugCrash(ctx context.Context, in *DebugCrashRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, StartedService_TriggerDebugCrash_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *startedServiceClient) TriggerOOMReport(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, StartedService_TriggerOOMReport_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *startedServiceClient) SubscribeConnections(ctx context.Context, in *SubscribeConnectionsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[ConnectionEvents], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &StartedService_ServiceDesc.Streams[5], StartedService_SubscribeConnections_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[SubscribeConnectionsRequest, ConnectionEvents]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type StartedService_SubscribeConnectionsClient = grpc.ServerStreamingClient[ConnectionEvents]

func (c *startedServiceClient) CloseConnection(ctx context.Context, in *CloseConnectionRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, StartedService_CloseConnection_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *startedServiceClient) CloseAllConnections(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, StartedService_CloseAllConnections_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *startedServiceClient) GetDeprecatedWarnings(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*DeprecatedWarnings, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(DeprecatedWarnings)
	err := c.cc.Invoke(ctx, StartedService_GetDeprecatedWarnings_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *startedServiceClient) GetStartedAt(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*StartedAt, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(StartedAt)
	err := c.cc.Invoke(ctx, StartedService_GetStartedAt_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *startedServiceClient) SubscribeOutbounds(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (grpc.ServerStreamingClient[OutboundList], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &StartedService_ServiceDesc.Streams[6], StartedService_SubscribeOutbounds_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[emptypb.Empty, OutboundList]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type StartedService_SubscribeOutboundsClient = grpc.ServerStreamingClient[OutboundList]

func (c *startedServiceClient) StartNetworkQualityTest(ctx context.Context, in *NetworkQualityTestRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[NetworkQualityTestProgress], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &StartedService_ServiceDesc.Streams[7], StartedService_StartNetworkQualityTest_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[NetworkQualityTestRequest, NetworkQualityTestProgress]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type StartedService_StartNetworkQualityTestClient = grpc.ServerStreamingClient[NetworkQualityTestProgress]

func (c *startedServiceClient) StartSTUNTest(ctx context.Context, in *STUNTestRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[STUNTestProgress], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &StartedService_ServiceDesc.Streams[8], StartedService_StartSTUNTest_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[STUNTestRequest, STUNTestProgress]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type StartedService_StartSTUNTestClient = grpc.ServerStreamingClient[STUNTestProgress]

func (c *startedServiceClient) SubscribeTailscaleStatus(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (grpc.ServerStreamingClient[TailscaleStatusUpdate], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &StartedService_ServiceDesc.Streams[9], StartedService_SubscribeTailscaleStatus_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[emptypb.Empty, TailscaleStatusUpdate]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type StartedService_SubscribeTailscaleStatusClient = grpc.ServerStreamingClient[TailscaleStatusUpdate]

func (c *startedServiceClient) StartTailscalePing(ctx context.Context, in *TailscalePingRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[TailscalePingResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &StartedService_ServiceDesc.Streams[10], StartedService_StartTailscalePing_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[TailscalePingRequest, TailscalePingResponse]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type StartedService_StartTailscalePingClient = grpc.ServerStreamingClient[TailscalePingResponse]

func (c *startedServiceClient) ListUsers(ctx context.Context, in *ListUsersRequest, opts ...grpc.CallOption) (*UserList, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(UserList)
	err := c.cc.Invoke(ctx, StartedService_ListUsers_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *startedServiceClient) GetUser(ctx context.Context, in *GetUserRequest, opts ...grpc.CallOption) (*UserInfo, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(UserInfo)
	err := c.cc.Invoke(ctx, StartedService_GetUser_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *startedServiceClient) AddUser(ctx context.Context, in *AddUserRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, StartedService_AddUser_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *startedServiceClient) UpdateUser(ctx context.Context, in *UpdateUserRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, StartedService_UpdateUser_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *startedServiceClient) DeleteUser(ctx context.Context, in *DeleteUserRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, StartedService_DeleteUser_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *startedServiceClient) GetInboundStats(ctx context.Context, in *GetInboundStatsRequest, opts ...grpc.CallOption) (*InboundStats, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(InboundStats)
	err := c.cc.Invoke(ctx, StartedService_GetInboundStats_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *startedServiceClient) ListInbounds(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*InboundList, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(InboundList)
	err := c.cc.Invoke(ctx, StartedService_ListInbounds_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *startedServiceClient) AddInbound(ctx context.Context, in *AddInboundRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, StartedService_AddInbound_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *startedServiceClient) RemoveInbound(ctx context.Context, in *RemoveInboundRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, StartedService_RemoveInbound_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// StartedServiceServer is the server API for StartedService service.
// All implementations must embed UnimplementedStartedServiceServer
// for forward compatibility.
type StartedServiceServer interface {
	StopService(context.Context, *emptypb.Empty) (*emptypb.Empty, error)
	ReloadService(context.Context, *emptypb.Empty) (*emptypb.Empty, error)
	SubscribeServiceStatus(*emptypb.Empty, grpc.ServerStreamingServer[ServiceStatus]) error
	SubscribeLog(*emptypb.Empty, grpc.ServerStreamingServer[Log]) error
	GetDefaultLogLevel(context.Context, *emptypb.Empty) (*DefaultLogLevel, error)
	ClearLogs(context.Context, *emptypb.Empty) (*emptypb.Empty, error)
	SubscribeStatus(*SubscribeStatusRequest, grpc.ServerStreamingServer[Status]) error
	SubscribeGroups(*emptypb.Empty, grpc.ServerStreamingServer[Groups]) error
	GetClashModeStatus(context.Context, *emptypb.Empty) (*ClashModeStatus, error)
	SubscribeClashMode(*emptypb.Empty, grpc.ServerStreamingServer[ClashMode]) error
	SetClashMode(context.Context, *ClashMode) (*emptypb.Empty, error)
	URLTest(context.Context, *URLTestRequest) (*emptypb.Empty, error)
	SelectOutbound(context.Context, *SelectOutboundRequest) (*emptypb.Empty, error)
	SetGroupExpand(context.Context, *SetGroupExpandRequest) (*emptypb.Empty, error)
	GetSystemProxyStatus(context.Context, *emptypb.Empty) (*SystemProxyStatus, error)
	SetSystemProxyEnabled(context.Context, *SetSystemProxyEnabledRequest) (*emptypb.Empty, error)
	TriggerDebugCrash(context.Context, *DebugCrashRequest) (*emptypb.Empty, error)
	TriggerOOMReport(context.Context, *emptypb.Empty) (*emptypb.Empty, error)
	SubscribeConnections(*SubscribeConnectionsRequest, grpc.ServerStreamingServer[ConnectionEvents]) error
	CloseConnection(context.Context, *CloseConnectionRequest) (*emptypb.Empty, error)
	CloseAllConnections(context.Context, *emptypb.Empty) (*emptypb.Empty, error)
	GetDeprecatedWarnings(context.Context, *emptypb.Empty) (*DeprecatedWarnings, error)
	GetStartedAt(context.Context, *emptypb.Empty) (*StartedAt, error)
	SubscribeOutbounds(*emptypb.Empty, grpc.ServerStreamingServer[OutboundList]) error
	StartNetworkQualityTest(*NetworkQualityTestRequest, grpc.ServerStreamingServer[NetworkQualityTestProgress]) error
	StartSTUNTest(*STUNTestRequest, grpc.ServerStreamingServer[STUNTestProgress]) error
	SubscribeTailscaleStatus(*emptypb.Empty, grpc.ServerStreamingServer[TailscaleStatusUpdate]) error
	StartTailscalePing(*TailscalePingRequest, grpc.ServerStreamingServer[TailscalePingResponse]) error
	// User management
	ListUsers(context.Context, *ListUsersRequest) (*UserList, error)
	GetUser(context.Context, *GetUserRequest) (*UserInfo, error)
	AddUser(context.Context, *AddUserRequest) (*emptypb.Empty, error)
	UpdateUser(context.Context, *UpdateUserRequest) (*emptypb.Empty, error)
	DeleteUser(context.Context, *DeleteUserRequest) (*emptypb.Empty, error)
	// Traffic statistics
	GetInboundStats(context.Context, *GetInboundStatsRequest) (*InboundStats, error)
	// Inbound management
	ListInbounds(context.Context, *emptypb.Empty) (*InboundList, error)
	AddInbound(context.Context, *AddInboundRequest) (*emptypb.Empty, error)
	RemoveInbound(context.Context, *RemoveInboundRequest) (*emptypb.Empty, error)
	mustEmbedUnimplementedStartedServiceServer()
}

// UnimplementedStartedServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedStartedServiceServer struct{}

func (UnimplementedStartedServiceServer) StopService(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
	return nil, status.Error(codes.Unimplemented, "method StopService not implemented")
}
func (UnimplementedStartedServiceServer) ReloadService(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
	return nil, status.Error(codes.Unimplemented, "method ReloadService not implemented")
}
func (UnimplementedStartedServiceServer) SubscribeServiceStatus(*emptypb.Empty, grpc.ServerStreamingServer[ServiceStatus]) error {
	return status.Error(codes.Unimplemented, "method SubscribeServiceStatus not implemented")
}
func (UnimplementedStartedServiceServer) SubscribeLog(*emptypb.Empty, grpc.ServerStreamingServer[Log]) error {
	return status.Error(codes.Unimplemented, "method SubscribeLog not implemented")
}
func (UnimplementedStartedServiceServer) GetDefaultLogLevel(context.Context, *emptypb.Empty) (*DefaultLogLevel, error) {
	return nil, status.Error(codes.Unimplemented, "method GetDefaultLogLevel not implemented")
}
func (UnimplementedStartedServiceServer) ClearLogs(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
	return nil, status.Error(codes.Unimplemented, "method ClearLogs not implemented")
}
func (UnimplementedStartedServiceServer) SubscribeStatus(*SubscribeStatusRequest, grpc.ServerStreamingServer[Status]) error {
	return status.Error(codes.Unimplemented, "method SubscribeStatus not implemented")
}
func (UnimplementedStartedServiceServer) SubscribeGroups(*emptypb.Empty, grpc.ServerStreamingServer[Groups]) error {
	return status.Error(codes.Unimplemented, "method SubscribeGroups not implemented")
}
func (UnimplementedStartedServiceServer) GetClashModeStatus(context.Context, *emptypb.Empty) (*ClashModeStatus, error) {
	return nil, status.Error(codes.Unimplemented, "method GetClashModeStatus not implemented")
}
func (UnimplementedStartedServiceServer) SubscribeClashMode(*emptypb.Empty, grpc.ServerStreamingServer[ClashMode]) error {
	return status.Error(codes.Unimplemented, "method SubscribeClashMode not implemented")
}
func (UnimplementedStartedServiceServer) SetClashMode(context.Context, *ClashMode) (*emptypb.Empty, error) {
	return nil, status.Error(codes.Unimplemented, "method SetClashMode not implemented")
}
func (UnimplementedStartedServiceServer) URLTest(context.Context, *URLTestRequest) (*emptypb.Empty, error) {
	return nil, status.Error(codes.Unimplemented, "method URLTest not implemented")
}
func (UnimplementedStartedServiceServer) SelectOutbound(context.Context, *SelectOutboundRequest) (*emptypb.Empty, error) {
	return nil, status.Error(codes.Unimplemented, "method SelectOutbound not implemented")
}
func (UnimplementedStartedServiceServer) SetGroupExpand(context.Context, *SetGroupExpandRequest) (*emptypb.Empty, error) {
	return nil, status.Error(codes.Unimplemented, "method SetGroupExpand not implemented")
}
func (UnimplementedStartedServiceServer) GetSystemProxyStatus(context.Context, *emptypb.Empty) (*SystemProxyStatus, error) {
	return nil, status.Error(codes.Unimplemented, "method GetSystemProxyStatus not implemented")
}
func (UnimplementedStartedServiceServer) SetSystemProxyEnabled(context.Context, *SetSystemProxyEnabledRequest) (*emptypb.Empty, error) {
	return nil, status.Error(codes.Unimplemented, "method SetSystemProxyEnabled not implemented")
}
func (UnimplementedStartedServiceServer) TriggerDebugCrash(context.Context, *DebugCrashRequest) (*emptypb.Empty, error) {
	return nil, status.Error(codes.Unimplemented, "method TriggerDebugCrash not implemented")
}
func (UnimplementedStartedServiceServer) TriggerOOMReport(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
	return nil, status.Error(codes.Unimplemented, "method TriggerOOMReport not implemented")
}
func (UnimplementedStartedServiceServer) SubscribeConnections(*SubscribeConnectionsRequest, grpc.ServerStreamingServer[ConnectionEvents]) error {
	return status.Error(codes.Unimplemented, "method SubscribeConnections not implemented")
}
func (UnimplementedStartedServiceServer) CloseConnection(context.Context, *CloseConnectionRequest) (*emptypb.Empty, error) {
	return nil, status.Error(codes.Unimplemented, "method CloseConnection not implemented")
}
func (UnimplementedStartedServiceServer) CloseAllConnections(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
	return nil, status.Error(codes.Unimplemented, "method CloseAllConnections not implemented")
}
func (UnimplementedStartedServiceServer) GetDeprecatedWarnings(context.Context, *emptypb.Empty) (*DeprecatedWarnings, error) {
	return nil, status.Error(codes.Unimplemented, "method GetDeprecatedWarnings not implemented")
}
func (UnimplementedStartedServiceServer) GetStartedAt(context.Context, *emptypb.Empty) (*StartedAt, error) {
	return nil, status.Error(codes.Unimplemented, "method GetStartedAt not implemented")
}
func (UnimplementedStartedServiceServer) SubscribeOutbounds(*emptypb.Empty, grpc.ServerStreamingServer[OutboundList]) error {
	return status.Error(codes.Unimplemented, "method SubscribeOutbounds not implemented")
}
func (UnimplementedStartedServiceServer) StartNetworkQualityTest(*NetworkQualityTestRequest, grpc.ServerStreamingServer[NetworkQualityTestProgress]) error {
	return status.Error(codes.Unimplemented, "method StartNetworkQualityTest not implemented")
}
func (UnimplementedStartedServiceServer) StartSTUNTest(*STUNTestRequest, grpc.ServerStreamingServer[STUNTestProgress]) error {
	return status.Error(codes.Unimplemented, "method StartSTUNTest not implemented")
}
func (UnimplementedStartedServiceServer) SubscribeTailscaleStatus(*emptypb.Empty, grpc.ServerStreamingServer[TailscaleStatusUpdate]) error {
	return status.Error(codes.Unimplemented, "method SubscribeTailscaleStatus not implemented")
}
func (UnimplementedStartedServiceServer) StartTailscalePing(*TailscalePingRequest, grpc.ServerStreamingServer[TailscalePingResponse]) error {
	return status.Error(codes.Unimplemented, "method StartTailscalePing not implemented")
}
func (UnimplementedStartedServiceServer) ListUsers(context.Context, *ListUsersRequest) (*UserList, error) {
	return nil, status.Error(codes.Unimplemented, "method ListUsers not implemented")
}
func (UnimplementedStartedServiceServer) GetUser(context.Context, *GetUserRequest) (*UserInfo, error) {
	return nil, status.Error(codes.Unimplemented, "method GetUser not implemented")
}
func (UnimplementedStartedServiceServer) AddUser(context.Context, *AddUserRequest) (*emptypb.Empty, error) {
	return nil, status.Error(codes.Unimplemented, "method AddUser not implemented")
}
func (UnimplementedStartedServiceServer) UpdateUser(context.Context, *UpdateUserRequest) (*emptypb.Empty, error) {
	return nil, status.Error(codes.Unimplemented, "method UpdateUser not implemented")
}
func (UnimplementedStartedServiceServer) DeleteUser(context.Context, *DeleteUserRequest) (*emptypb.Empty, error) {
	return nil, status.Error(codes.Unimplemented, "method DeleteUser not implemented")
}
func (UnimplementedStartedServiceServer) GetInboundStats(context.Context, *GetInboundStatsRequest) (*InboundStats, error) {
	return nil, status.Error(codes.Unimplemented, "method GetInboundStats not implemented")
}
func (UnimplementedStartedServiceServer) ListInbounds(context.Context, *emptypb.Empty) (*InboundList, error) {
	return nil, status.Error(codes.Unimplemented, "method ListInbounds not implemented")
}
func (UnimplementedStartedServiceServer) AddInbound(context.Context, *AddInboundRequest) (*emptypb.Empty, error) {
	return nil, status.Error(codes.Unimplemented, "method AddInbound not implemented")
}
func (UnimplementedStartedServiceServer) RemoveInbound(context.Context, *RemoveInboundRequest) (*emptypb.Empty, error) {
	return nil, status.Error(codes.Unimplemented, "method RemoveInbound not implemented")
}
func (UnimplementedStartedServiceServer) mustEmbedUnimplementedStartedServiceServer() {}
func (UnimplementedStartedServiceServer) testEmbeddedByValue()                        {}

// UnsafeStartedServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to StartedServiceServer will
// result in compilation errors.
type UnsafeStartedServiceServer interface {
	mustEmbedUnimplementedStartedServiceServer()
}

func RegisterStartedServiceServer(s grpc.ServiceRegistrar, srv StartedServiceServer) {
	// If the following call panics, it indicates UnimplementedStartedServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&StartedService_ServiceDesc, srv)
}

func _StartedService_StopService_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StartedServiceServer).StopService(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: StartedService_StopService_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StartedServiceServer).StopService(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _StartedService_ReloadService_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StartedServiceServer).ReloadService(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: StartedService_ReloadService_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StartedServiceServer).ReloadService(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _StartedService_SubscribeServiceStatus_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(emptypb.Empty)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(StartedServiceServer).SubscribeServiceStatus(m, &grpc.GenericServerStream[emptypb.Empty, ServiceStatus]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type StartedService_SubscribeServiceStatusServer = grpc.ServerStreamingServer[ServiceStatus]

func _StartedService_SubscribeLog_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(emptypb.Empty)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(StartedServiceServer).SubscribeLog(m, &grpc.GenericServerStream[emptypb.Empty, Log]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type StartedService_SubscribeLogServer = grpc.ServerStreamingServer[Log]

func _StartedService_GetDefaultLogLevel_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StartedServiceServer).GetDefaultLogLevel(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: StartedService_GetDefaultLogLevel_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StartedServiceServer).GetDefaultLogLevel(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _StartedService_ClearLogs_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StartedServiceServer).ClearLogs(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: StartedService_ClearLogs_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StartedServiceServer).ClearLogs(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _StartedService_SubscribeStatus_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(SubscribeStatusRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(StartedServiceServer).SubscribeStatus(m, &grpc.GenericServerStream[SubscribeStatusRequest, Status]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type StartedService_SubscribeStatusServer = grpc.ServerStreamingServer[Status]

func _StartedService_SubscribeGroups_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(emptypb.Empty)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(StartedServiceServer).SubscribeGroups(m, &grpc.GenericServerStream[emptypb.Empty, Groups]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type StartedService_SubscribeGroupsServer = grpc.ServerStreamingServer[Groups]

func _StartedService_GetClashModeStatus_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StartedServiceServer).GetClashModeStatus(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: StartedService_GetClashModeStatus_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StartedServiceServer).GetClashModeStatus(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _StartedService_SubscribeClashMode_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(emptypb.Empty)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(StartedServiceServer).SubscribeClashMode(m, &grpc.GenericServerStream[emptypb.Empty, ClashMode]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type StartedService_SubscribeClashModeServer = grpc.ServerStreamingServer[ClashMode]

func _StartedService_SetClashMode_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ClashMode)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StartedServiceServer).SetClashMode(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: StartedService_SetClashMode_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StartedServiceServer).SetClashMode(ctx, req.(*ClashMode))
	}
	return interceptor(ctx, in, info, handler)
}

func _StartedService_URLTest_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(URLTestRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StartedServiceServer).URLTest(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: StartedService_URLTest_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StartedServiceServer).URLTest(ctx, req.(*URLTestRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _StartedService_SelectOutbound_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SelectOutboundRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StartedServiceServer).SelectOutbound(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: StartedService_SelectOutbound_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StartedServiceServer).SelectOutbound(ctx, req.(*SelectOutboundRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _StartedService_SetGroupExpand_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SetGroupExpandRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StartedServiceServer).SetGroupExpand(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: StartedService_SetGroupExpand_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StartedServiceServer).SetGroupExpand(ctx, req.(*SetGroupExpandRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _StartedService_GetSystemProxyStatus_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StartedServiceServer).GetSystemProxyStatus(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: StartedService_GetSystemProxyStatus_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StartedServiceServer).GetSystemProxyStatus(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _StartedService_SetSystemProxyEnabled_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SetSystemProxyEnabledRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StartedServiceServer).SetSystemProxyEnabled(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: StartedService_SetSystemProxyEnabled_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StartedServiceServer).SetSystemProxyEnabled(ctx, req.(*SetSystemProxyEnabledRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _StartedService_TriggerDebugCrash_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DebugCrashRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StartedServiceServer).TriggerDebugCrash(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: StartedService_TriggerDebugCrash_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StartedServiceServer).TriggerDebugCrash(ctx, req.(*DebugCrashRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _StartedService_TriggerOOMReport_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StartedServiceServer).TriggerOOMReport(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: StartedService_TriggerOOMReport_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StartedServiceServer).TriggerOOMReport(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _StartedService_SubscribeConnections_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(SubscribeConnectionsRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(StartedServiceServer).SubscribeConnections(m, &grpc.GenericServerStream[SubscribeConnectionsRequest, ConnectionEvents]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type StartedService_SubscribeConnectionsServer = grpc.ServerStreamingServer[ConnectionEvents]

func _StartedService_CloseConnection_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CloseConnectionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StartedServiceServer).CloseConnection(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: StartedService_CloseConnection_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StartedServiceServer).CloseConnection(ctx, req.(*CloseConnectionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _StartedService_CloseAllConnections_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StartedServiceServer).CloseAllConnections(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: StartedService_CloseAllConnections_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StartedServiceServer).CloseAllConnections(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _StartedService_GetDeprecatedWarnings_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StartedServiceServer).GetDeprecatedWarnings(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: StartedService_GetDeprecatedWarnings_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StartedServiceServer).GetDeprecatedWarnings(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _StartedService_GetStartedAt_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StartedServiceServer).GetStartedAt(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: StartedService_GetStartedAt_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StartedServiceServer).GetStartedAt(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _StartedService_SubscribeOutbounds_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(emptypb.Empty)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(StartedServiceServer).SubscribeOutbounds(m, &grpc.GenericServerStream[emptypb.Empty, OutboundList]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type StartedService_SubscribeOutboundsServer = grpc.ServerStreamingServer[OutboundList]

func _StartedService_StartNetworkQualityTest_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(NetworkQualityTestRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(StartedServiceServer).StartNetworkQualityTest(m, &grpc.GenericServerStream[NetworkQualityTestRequest, NetworkQualityTestProgress]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type StartedService_StartNetworkQualityTestServer = grpc.ServerStreamingServer[NetworkQualityTestProgress]

func _StartedService_StartSTUNTest_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(STUNTestRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(StartedServiceServer).StartSTUNTest(m, &grpc.GenericServerStream[STUNTestRequest, STUNTestProgress]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type StartedService_StartSTUNTestServer = grpc.ServerStreamingServer[STUNTestProgress]

func _StartedService_SubscribeTailscaleStatus_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(emptypb.Empty)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(StartedServiceServer).SubscribeTailscaleStatus(m, &grpc.GenericServerStream[emptypb.Empty, TailscaleStatusUpdate]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type StartedService_SubscribeTailscaleStatusServer = grpc.ServerStreamingServer[TailscaleStatusUpdate]

func _StartedService_StartTailscalePing_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(TailscalePingRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(StartedServiceServer).StartTailscalePing(m, &grpc.GenericServerStream[TailscalePingRequest, TailscalePingResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type StartedService_StartTailscalePingServer = grpc.ServerStreamingServer[TailscalePingResponse]

func _StartedService_ListUsers_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListUsersRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StartedServiceServer).ListUsers(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: StartedService_ListUsers_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StartedServiceServer).ListUsers(ctx, req.(*ListUsersRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _StartedService_GetUser_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetUserRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StartedServiceServer).GetUser(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: StartedService_GetUser_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StartedServiceServer).GetUser(ctx, req.(*GetUserRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _StartedService_AddUser_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AddUserRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StartedServiceServer).AddUser(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: StartedService_AddUser_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StartedServiceServer).AddUser(ctx, req.(*AddUserRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _StartedService_UpdateUser_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UpdateUserRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StartedServiceServer).UpdateUser(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: StartedService_UpdateUser_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StartedServiceServer).UpdateUser(ctx, req.(*UpdateUserRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _StartedService_DeleteUser_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteUserRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StartedServiceServer).DeleteUser(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: StartedService_DeleteUser_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StartedServiceServer).DeleteUser(ctx, req.(*DeleteUserRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _StartedService_GetInboundStats_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetInboundStatsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StartedServiceServer).GetInboundStats(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: StartedService_GetInboundStats_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StartedServiceServer).GetInboundStats(ctx, req.(*GetInboundStatsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _StartedService_ListInbounds_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StartedServiceServer).ListInbounds(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: StartedService_ListInbounds_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StartedServiceServer).ListInbounds(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _StartedService_AddInbound_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AddInboundRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StartedServiceServer).AddInbound(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: StartedService_AddInbound_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StartedServiceServer).AddInbound(ctx, req.(*AddInboundRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _StartedService_RemoveInbound_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RemoveInboundRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StartedServiceServer).RemoveInbound(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: StartedService_RemoveInbound_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StartedServiceServer).RemoveInbound(ctx, req.(*RemoveInboundRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// StartedService_ServiceDesc is the grpc.ServiceDesc for StartedService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var StartedService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "daemon.StartedService",
	HandlerType: (*StartedServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "StopService",
			Handler:    _StartedService_StopService_Handler,
		},
		{
			MethodName: "ReloadService",
			Handler:    _StartedService_ReloadService_Handler,
		},
		{
			MethodName: "GetDefaultLogLevel",
			Handler:    _StartedService_GetDefaultLogLevel_Handler,
		},
		{
			MethodName: "ClearLogs",
			Handler:    _StartedService_ClearLogs_Handler,
		},
		{
			MethodName: "GetClashModeStatus",
			Handler:    _StartedService_GetClashModeStatus_Handler,
		},
		{
			MethodName: "SetClashMode",
			Handler:    _StartedService_SetClashMode_Handler,
		},
		{
			MethodName: "URLTest",
			Handler:    _StartedService_URLTest_Handler,
		},
		{
			MethodName: "SelectOutbound",
			Handler:    _StartedService_SelectOutbound_Handler,
		},
		{
			MethodName: "SetGroupExpand",
			Handler:    _StartedService_SetGroupExpand_Handler,
		},
		{
			MethodName: "GetSystemProxyStatus",
			Handler:    _StartedService_GetSystemProxyStatus_Handler,
		},
		{
			MethodName: "SetSystemProxyEnabled",
			Handler:    _StartedService_SetSystemProxyEnabled_Handler,
		},
		{
			MethodName: "TriggerDebugCrash",
			Handler:    _StartedService_TriggerDebugCrash_Handler,
		},
		{
			MethodName: "TriggerOOMReport",
			Handler:    _StartedService_TriggerOOMReport_Handler,
		},
		{
			MethodName: "CloseConnection",
			Handler:    _StartedService_CloseConnection_Handler,
		},
		{
			MethodName: "CloseAllConnections",
			Handler:    _StartedService_CloseAllConnections_Handler,
		},
		{
			MethodName: "GetDeprecatedWarnings",
			Handler:    _StartedService_GetDeprecatedWarnings_Handler,
		},
		{
			MethodName: "GetStartedAt",
			Handler:    _StartedService_GetStartedAt_Handler,
		},
		{
			MethodName: "ListUsers",
			Handler:    _StartedService_ListUsers_Handler,
		},
		{
			MethodName: "GetUser",
			Handler:    _StartedService_GetUser_Handler,
		},
		{
			MethodName: "AddUser",
			Handler:    _StartedService_AddUser_Handler,
		},
		{
			MethodName: "UpdateUser",
			Handler:    _StartedService_UpdateUser_Handler,
		},
		{
			MethodName: "DeleteUser",
			Handler:    _StartedService_DeleteUser_Handler,
		},
		{
			MethodName: "GetInboundStats",
			Handler:    _StartedService_GetInboundStats_Handler,
		},
		{
			MethodName: "ListInbounds",
			Handler:    _StartedService_ListInbounds_Handler,
		},
		{
			MethodName: "AddInbound",
			Handler:    _StartedService_AddInbound_Handler,
		},
		{
			MethodName: "RemoveInbound",
			Handler:    _StartedService_RemoveInbound_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "SubscribeServiceStatus",
			Handler:       _StartedService_SubscribeServiceStatus_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "SubscribeLog",
			Handler:       _StartedService_SubscribeLog_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "SubscribeStatus",
			Handler:       _StartedService_SubscribeStatus_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "SubscribeGroups",
			Handler:       _StartedService_SubscribeGroups_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "SubscribeClashMode",
			Handler:       _StartedService_SubscribeClashMode_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "SubscribeConnections",
			Handler:       _StartedService_SubscribeConnections_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "SubscribeOutbounds",
			Handler:       _StartedService_SubscribeOutbounds_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "StartNetworkQualityTest",
			Handler:       _StartedService_StartNetworkQualityTest_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "StartSTUNTest",
			Handler:       _StartedService_StartSTUNTest_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "SubscribeTailscaleStatus",
			Handler:       _StartedService_SubscribeTailscaleStatus_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "StartTailscalePing",
			Handler:       _StartedService_StartTailscalePing_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "daemon/started_service.proto",
}
