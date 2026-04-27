package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	runtimeDebug "runtime/debug"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/daemon"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/service/ssmapi"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/json"
	"github.com/sagernet/sing/common/json/badjson"
	"github.com/sagernet/sing/service"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	grpcListen string
	grpcSecret string
)

var commandRun = &cobra.Command{
	Use:   "run",
	Short: "Run service",
	Run: func(cmd *cobra.Command, args []string) {
		err := run()
		if err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	commandRun.Flags().StringVar(&grpcListen, "grpc-listen", "", "gRPC control server listen address (e.g., unix:/tmp/sing-box.sock or 127.0.0.1:8080)")
	commandRun.Flags().StringVar(&grpcSecret, "grpc-secret", "", "gRPC authentication secret (requires --grpc-listen)")
	mainCommand.AddCommand(commandRun)
}

// cliControlServer implements daemon.StartedServiceServer for the CLI context.
type cliControlServer struct {
	daemon.UnimplementedStartedServiceServer
	box          *box.Box
	cacheFile    adapter.CacheFile
	userAccess   sync.Mutex
	userManagers map[string]*ssmapi.UserManager
	configPath   string

	// Config persistence
	configAccess   sync.Mutex
	configOptions  *option.Options
	inboundOptions map[string]*option.Inbound
}

func newCLIControlServer(box *box.Box, configPath string) *cliControlServer {
	return &cliControlServer{
		box:            box,
		userManagers:   make(map[string]*ssmapi.UserManager),
		configPath:     configPath,
		inboundOptions: make(map[string]*option.Inbound),
	}
}

func unaryAuthInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if grpcSecret == "" {
		return handler(ctx, req)
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, grpcAuthErr("missing metadata")
	}
	values := md.Get("x-command-secret")
	if len(values) == 0 {
		return nil, grpcAuthErr("missing authentication secret")
	}
	if values[0] != grpcSecret {
		return nil, grpcAuthErr("invalid authentication secret")
	}
	return handler(ctx, req)
}

func streamAuthInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if grpcSecret == "" {
		return handler(srv, ss)
	}
	md, ok := metadata.FromIncomingContext(ss.Context())
	if !ok {
		return grpcAuthErr("missing metadata")
	}
	values := md.Get("x-command-secret")
	if len(values) == 0 {
		return grpcAuthErr("missing authentication secret")
	}
	if values[0] != grpcSecret {
		return grpcAuthErr("invalid authentication secret")
	}
	return handler(srv, ss)
}

func grpcAuthErr(msg string) error {
	return status.Error(codes.Unauthenticated, msg)
}

func (s *cliControlServer) setBox(box *box.Box) {
	s.box = box
}

func (s *cliControlServer) setCacheFile(cacheFile adapter.CacheFile) {
	s.cacheFile = cacheFile
}

func (s *cliControlServer) saveAllStats() {
	s.userAccess.Lock()
	defer s.userAccess.Unlock()
	if s.cacheFile == nil {
		return
	}
	for tag, um := range s.userManagers {
		users := um.List()
		userNames := make([]string, len(users))
		for i, u := range users {
			userNames[i] = u.UserName
		}
		_ = ssmapi.SaveStats(um.TrafficManager(), userNames, s.cacheFile, tag)
	}
}

func (s *cliControlServer) storeConfig(opts *option.Options) {
	s.configAccess.Lock()
	defer s.configAccess.Unlock()

	s.configOptions = opts
	s.inboundOptions = make(map[string]*option.Inbound, len(opts.Inbounds))
	for i := range opts.Inbounds {
		tag := opts.Inbounds[i].Tag
		if tag == "" {
			continue
		}
		s.inboundOptions[tag] = &opts.Inbounds[i]
	}
}

func (s *cliControlServer) persistConfig() error {
	s.configAccess.Lock()
	configPath := s.configPath
	configOpts := s.configOptions
	inboundOpts := s.inboundOptions
	s.configAccess.Unlock()

	if configPath == "" || configOpts == nil {
		return nil
	}

	if s.box == nil {
		return nil
	}

	currentInbounds := s.box.Inbound().Inbounds()
	newInboundOpts := make([]option.Inbound, 0, len(currentInbounds))
	for _, inbound := range currentInbounds {
		tag := inbound.Tag()
		if existing, ok := inboundOpts[tag]; ok {
			newInboundOpts = append(newInboundOpts, *existing)
		} else {
			newInboundOpts = append(newInboundOpts, option.Inbound{
				Type: inbound.Type(),
				Tag:  tag,
			})
		}
	}
	configOpts.Inbounds = newInboundOpts

	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetIndent("", "  ")
	err := encoder.Encode(configOpts)
	if err != nil {
		return err
	}

	return os.WriteFile(configPath, buffer.Bytes(), 0o644)
}

func (s *cliControlServer) ensureUserManager(inboundTag string) (*ssmapi.UserManager, error) {
	s.userAccess.Lock()
	if um, exists := s.userManagers[inboundTag]; exists {
		s.userAccess.Unlock()
		return um, nil
	}
	s.userAccess.Unlock()

	if s.box == nil {
		return nil, os.ErrInvalid
	}

	inbound, found := s.box.Inbound().Get(inboundTag)
	if !found {
		return nil, status.Error(codes.NotFound, "inbound not found: "+inboundTag)
	}

	managedServer, isManaged := inbound.(adapter.ManagedSSMServer)
	if !isManaged {
		return nil, status.Error(codes.FailedPrecondition, "inbound does not support user management: "+inboundTag)
	}

	traffic := ssmapi.NewTrafficManager()
	managedServer.SetTracker(traffic)
	um := ssmapi.NewUserManager(managedServer, traffic)

	s.configAccess.Lock()
	if opts, ok := s.inboundOptions[inboundTag]; ok && opts.Options != nil {
		if users := ssmapi.ExtractUsersFromOptions(opts.Options); users != nil {
			um.LoadUsers(users)
		}
	}
	s.configAccess.Unlock()

	if s.cacheFile != nil {
		_ = ssmapi.LoadStats(traffic, s.cacheFile, inboundTag)
	}

	s.userAccess.Lock()
	if existing, exists := s.userManagers[inboundTag]; exists {
		s.userAccess.Unlock()
		return existing, nil
	}
	s.userManagers[inboundTag] = um
	s.userAccess.Unlock()
	return um, nil
}

func (s *cliControlServer) ListUsers(ctx context.Context, request *daemon.ListUsersRequest) (*daemon.UserList, error) {
	log.Info(fmt.Sprintf("listing users for inbound %q", request.InboundTag))
	um, err := s.ensureUserManager(request.InboundTag)
	if err != nil {
		return nil, err
	}
	users := um.List()
	protoUsers := make([]*daemon.UserInfo, 0, len(users))
	for _, u := range users {
		protoUsers = append(protoUsers, &daemon.UserInfo{
			UserName: u.UserName,
			Password: u.Password,
		})
	}
	return &daemon.UserList{Users: protoUsers}, nil
}

func (s *cliControlServer) GetUser(ctx context.Context, request *daemon.GetUserRequest) (*daemon.UserInfo, error) {
	log.Info(fmt.Sprintf("getting user %q from inbound %q", request.UserName, request.InboundTag))
	um, err := s.ensureUserManager(request.InboundTag)
	if err != nil {
		return nil, err
	}
	password, found := um.Get(request.UserName)
	if !found {
		return nil, status.Error(codes.NotFound, "user not found: "+request.UserName)
	}
	return &daemon.UserInfo{
		UserName: request.UserName,
		Password: password,
	}, nil
}

func (s *cliControlServer) AddUser(ctx context.Context, request *daemon.AddUserRequest) (*emptypb.Empty, error) {
	log.Info(fmt.Sprintf("adding user %q to inbound %q", request.UserName, request.InboundTag))
	um, err := s.ensureUserManager(request.InboundTag)
	if err != nil {
		return nil, err
	}
	err = um.Add(request.UserName, request.Password)
	if err != nil {
		return nil, gRPCError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *cliControlServer) UpdateUser(ctx context.Context, request *daemon.UpdateUserRequest) (*emptypb.Empty, error) {
	log.Info(fmt.Sprintf("updating user %q on inbound %q", request.UserName, request.InboundTag))
	um, err := s.ensureUserManager(request.InboundTag)
	if err != nil {
		return nil, err
	}
	_, found := um.Get(request.UserName)
	if !found {
		return nil, status.Error(codes.NotFound, "user not found: "+request.UserName)
	}
	err = um.Update(request.UserName, request.Password)
	if err != nil {
		return nil, gRPCError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *cliControlServer) DeleteUser(ctx context.Context, request *daemon.DeleteUserRequest) (*emptypb.Empty, error) {
	log.Info(fmt.Sprintf("deleting user %q from inbound %q", request.UserName, request.InboundTag))
	um, err := s.ensureUserManager(request.InboundTag)
	if err != nil {
		return nil, err
	}
	_, found := um.Get(request.UserName)
	if !found {
		return nil, status.Error(codes.NotFound, "user not found: "+request.UserName)
	}
	err = um.Delete(request.UserName)
	if err != nil {
		return nil, gRPCError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *cliControlServer) GetInboundStats(ctx context.Context, request *daemon.GetInboundStatsRequest) (*daemon.InboundStats, error) {
	um, err := s.ensureUserManager(request.InboundTag)
	if err != nil {
		return nil, err
	}
	uplinkBytes, downlinkBytes, uplinkPackets, downlinkPackets, tcpSessions, udpSessions :=
		um.TrafficManager().ReadGlobal(request.Clear)
	users := um.List()
	um.TrafficManager().ReadUsers(users, request.Clear)

	if s.cacheFile != nil {
		userNames := make([]string, len(users))
		for i, u := range users {
			userNames[i] = u.UserName
		}
		_ = ssmapi.SaveStats(um.TrafficManager(), userNames, s.cacheFile, request.InboundTag)
	}

	protoUsers := make([]*daemon.UserInfo, 0, len(users))
	for _, u := range users {
		protoUsers = append(protoUsers, &daemon.UserInfo{
			UserName:        u.UserName,
			Uplink:          u.UplinkBytes,
			Downlink:        u.DownlinkBytes,
			UplinkPackets:   u.UplinkPackets,
			DownlinkPackets: u.DownlinkPackets,
			TcpSessions:     u.TCPSessions,
			UdpSessions:     u.UDPSessions,
		})
	}
	return &daemon.InboundStats{
		UplinkBytes:     uplinkBytes,
		DownlinkBytes:   downlinkBytes,
		UplinkPackets:   uplinkPackets,
		DownlinkPackets: downlinkPackets,
		TcpSessions:     tcpSessions,
		UdpSessions:     udpSessions,
		Users:           protoUsers,
	}, nil
}

func (s *cliControlServer) ListInbounds(ctx context.Context, _ *emptypb.Empty) (*daemon.InboundList, error) {
	log.Info("listing inbounds")
	if s.box == nil {
		return &daemon.InboundList{}, nil
	}
	inbounds := s.box.Inbound().Inbounds()
	list := make([]*daemon.InboundInfo, 0, len(inbounds))
	for _, inbound := range inbounds {
		_, isManaged := inbound.(adapter.ManagedSSMServer)
		list = append(list, &daemon.InboundInfo{
			Tag:          inbound.Tag(),
			Type:         inbound.Type(),
			IsManagedSsm: isManaged,
		})
	}
	return &daemon.InboundList{Inbounds: list}, nil
}

func (s *cliControlServer) AddInbound(ctx context.Context, request *daemon.AddInboundRequest) (*emptypb.Empty, error) {
	log.Info(fmt.Sprintf("adding inbound %q of type %q", request.Tag, request.Type))
	if s.box == nil {
		return nil, os.ErrInvalid
	}

	// Inject type field into options JSON so UnmarshalJSONContext can find it
	fullJSON := `{"type":"` + request.Type + `",` + request.OptionsJson[1:]
	var inboundOption option.Inbound
	err := json.UnmarshalContext(globalCtx, []byte(fullJSON), &inboundOption)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid options JSON: %v", err)
	}

	logger := s.box.LogFactory().NewLogger("inbound/" + request.Type + "[" + request.Tag + "]")
	err = s.box.Inbound().Create(globalCtx, s.box.Router(), logger, request.Tag, request.Type, inboundOption.Options)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create inbound: %v", err)
	}

	// Store full inbound option for persistence
	inboundOption.Tag = request.Tag
	s.configAccess.Lock()
	if s.inboundOptions != nil {
		cloned := inboundOption
		s.inboundOptions[request.Tag] = &cloned
	}
	s.configAccess.Unlock()

	err = s.persistConfig()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "persist config: %v", err)
	}

	return &emptypb.Empty{}, nil
}

func (s *cliControlServer) RemoveInbound(ctx context.Context, request *daemon.RemoveInboundRequest) (*emptypb.Empty, error) {
	log.Info(fmt.Sprintf("removing inbound %q", request.Tag))
	if s.box == nil {
		return nil, os.ErrInvalid
	}
	_, found := s.box.Inbound().Get(request.Tag)
	if !found {
		return nil, status.Error(codes.NotFound, "inbound not found: "+request.Tag)
	}
	err := s.box.Inbound().Remove(request.Tag)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "remove inbound: %v", err)
	}

	// Remove from stored options
	s.configAccess.Lock()
	if s.inboundOptions != nil {
		delete(s.inboundOptions, request.Tag)
	}
	s.configAccess.Unlock()

	err = s.persistConfig()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "persist config: %v", err)
	}

	return &emptypb.Empty{}, nil
}

type OptionsEntry struct {
	content []byte
	path    string
	options option.Options
}

func readConfigAt(path string) (*OptionsEntry, error) {
	var (
		configContent []byte
		err           error
	)
	if path == "stdin" {
		configContent, err = io.ReadAll(os.Stdin)
	} else {
		configContent, err = os.ReadFile(path)
	}
	if err != nil {
		return nil, E.Cause(err, "read config at ", path)
	}
	options, err := json.UnmarshalExtendedContext[option.Options](globalCtx, configContent)
	if err != nil {
		return nil, E.Cause(err, "decode config at ", path)
	}
	return &OptionsEntry{
		content: configContent,
		path:    path,
		options: options,
	}, nil
}

func readConfig() ([]*OptionsEntry, error) {
	var optionsList []*OptionsEntry
	for _, path := range configPaths {
		optionsEntry, err := readConfigAt(path)
		if err != nil {
			return nil, err
		}
		optionsList = append(optionsList, optionsEntry)
	}
	for _, directory := range configDirectories {
		entries, err := os.ReadDir(directory)
		if err != nil {
			return nil, E.Cause(err, "read config directory at ", directory)
		}
		for _, entry := range entries {
			if !strings.HasSuffix(entry.Name(), ".json") || entry.IsDir() {
				continue
			}
			optionsEntry, err := readConfigAt(filepath.Join(directory, entry.Name()))
			if err != nil {
				return nil, err
			}
			optionsList = append(optionsList, optionsEntry)
		}
	}
	sort.Slice(optionsList, func(i, j int) bool {
		return optionsList[i].path < optionsList[j].path
	})
	return optionsList, nil
}

func readConfigAndMerge() (option.Options, error) {
	optionsList, err := readConfig()
	if err != nil {
		return option.Options{}, err
	}
	if len(optionsList) == 1 {
		return optionsList[0].options, nil
	}
	var mergedMessage json.RawMessage
	for _, options := range optionsList {
		mergedMessage, err = badjson.MergeJSON(globalCtx, options.options.RawMessage, mergedMessage, false)
		if err != nil {
			return option.Options{}, E.Cause(err, "merge config at ", options.path)
		}
	}
	var mergedOptions option.Options
	err = mergedOptions.UnmarshalJSONContext(globalCtx, mergedMessage)
	if err != nil {
		return option.Options{}, E.Cause(err, "unmarshal merged config")
	}
	return mergedOptions, nil
}

var cliCtrlServer *cliControlServer

func create() (*box.Box, context.CancelFunc, error) {
	options, err := readConfigAndMerge()
	if err != nil {
		return nil, nil, err
	}
	if disableColor {
		if options.Log == nil {
			options.Log = &option.LogOptions{}
		}
		options.Log.DisableColor = true
	}

	// Store config for runtime persistence
	if cliCtrlServer != nil {
		cliCtrlServer.storeConfig(&options)
		cliCtrlServer.userAccess.Lock()
		cliCtrlServer.userManagers = make(map[string]*ssmapi.UserManager)
		cliCtrlServer.userAccess.Unlock()
	}

	ctx, cancel := context.WithCancel(globalCtx)
	instance, err := box.New(box.Options{
		Context: ctx,
		Options: options,
	})
	if err != nil {
		cancel()
		return nil, nil, E.Cause(err, "create service")
	}

	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer func() {
		signal.Stop(osSignals)
		close(osSignals)
	}()
	startCtx, finishStart := context.WithCancel(context.Background())
	go func() {
		_, loaded := <-osSignals
		if loaded {
			cancel()
			closeMonitor(startCtx)
		}
	}()
	err = instance.Start()
	finishStart()
	if err != nil {
		cancel()
		return nil, nil, E.Cause(err, "start service")
	}

	// Register with gRPC control server
	if cliCtrlServer != nil {
		cliCtrlServer.setBox(instance)
		cliCtrlServer.setCacheFile(service.FromContext[adapter.CacheFile](ctx))
	}

	// Update global std logger so gRPC handler log messages respect
	// the configured timestamp/format settings.
	log.SetStdLogger(instance.LogFactory().Logger())

	return instance, cancel, nil
}

func run() error {
	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer signal.Stop(osSignals)

	// Read config early to resolve gRPC settings and set up logging.
	// CLI flags take precedence over config file.
	grpcListenAddr := grpcListen
	grpcAuthSecret := grpcSecret
	if options, err := readConfigAndMerge(); err == nil {
		if options.Log != nil && !options.Log.Disabled {
			logFormatter := log.Formatter{
				BaseTime:         time.Now(),
				DisableColors:    options.Log.DisableColor,
				DisableTimestamp: !options.Log.Timestamp,
				FullTimestamp:    options.Log.Timestamp,
				TimestampFormat:  "-0700 2006-01-02 15:04:05",
			}
			log.SetStdLogger(log.NewDefaultFactory(globalCtx, logFormatter, os.Stderr, "", nil, false).Logger())
		}
		if options.GRPC != nil {
			if grpcListenAddr == "" && options.GRPC.Listen != "" {
				grpcListenAddr = options.GRPC.Listen
			}
			if grpcAuthSecret == "" && options.GRPC.Secret != "" {
				grpcAuthSecret = options.GRPC.Secret
			}
		}
	}
	// Update package-level var so auth interceptors use the resolved secret
	grpcSecret = grpcAuthSecret

	// Start gRPC control server if configured
	var grpcListener net.Listener
	var grpcServer *grpc.Server
	if grpcListenAddr != "" {
		var err error
		cliCtrlServer = newCLIControlServer(nil, configPaths[0])
		if strings.HasPrefix(grpcListenAddr, "unix:") {
			sockPath := grpcListenAddr[5:]
			_ = os.Remove(sockPath)
			grpcListener, err = net.Listen("unix", sockPath)
		} else {
			grpcListener, err = net.Listen("tcp", grpcListenAddr)
		}
		if err != nil {
			return E.Cause(err, "gRPC listen on ", grpcListenAddr)
		}
		serverOpts := []grpc.ServerOption{}
		if grpcSecret != "" {
			serverOpts = append(serverOpts, grpc.UnaryInterceptor(unaryAuthInterceptor), grpc.StreamInterceptor(streamAuthInterceptor))
		}
		grpcServer = grpc.NewServer(serverOpts...)
		daemon.RegisterStartedServiceServer(grpcServer, cliCtrlServer)
		reflection.Register(grpcServer)
		go grpcServer.Serve(grpcListener)
		log.Info(fmt.Sprintf("gRPC command server started on %s", grpcListenAddr))
	}

	for {
		instance, cancel, err := create()
		if err != nil {
			if grpcServer != nil {
				grpcServer.Stop()
				grpcListener.Close()
			}
			return err
		}
		runtimeDebug.FreeOSMemory()
		for {
			osSignal := <-osSignals
			if cliCtrlServer != nil {
				cliCtrlServer.saveAllStats()
			}
			if osSignal == syscall.SIGHUP {
				err = check()
				if err != nil {
					log.Error(E.Cause(err, "reload service"))
					continue
				}
			}
			cancel()
			closeCtx, closed := context.WithCancel(context.Background())
			go closeMonitor(closeCtx)
			err = instance.Close()
			closed()
			// Clear gRPC box reference
			if cliCtrlServer != nil {
				cliCtrlServer.setBox(nil)
			}
			log.SetStdLogger(log.NewDefaultFactory(context.Background(), log.Formatter{BaseTime: time.Now()}, os.Stderr, "", nil, false).Logger())
			if osSignal != syscall.SIGHUP {
				if err != nil {
					log.Error(E.Cause(err, "sing-box did not closed properly"))
				}
				if grpcServer != nil {
					grpcServer.Stop()
					grpcListener.Close()
				}
				return nil
			}
			break
		}
	}
}

func gRPCError(err error) error {
	if strings.Contains(err.Error(), "already exists") {
		return status.Error(codes.AlreadyExists, err.Error())
	}
	if strings.Contains(err.Error(), "not found") {
		return status.Error(codes.NotFound, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

func closeMonitor(ctx context.Context) {
	time.Sleep(C.FatalStopTimeout)
	select {
	case <-ctx.Done():
		return
	default:
	}
	log.Fatal("sing-box did not close!")
}
