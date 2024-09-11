package main

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/carlakc/lrc"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/lightningequipment/circuitbreaker/circuitbreakerrpc"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/urfave/cli"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

var errUserExit = errors.New("user requested termination")

// maxGrpcMsgSize is used when we configure both server and clients to allow sending and
// receiving at most 32 MB GRPC messages.
//
// This value is based on the default number of forwarding history entries that we'll
// store in the database, as this is the largest query we currently make (~13 MB of data)
// plus some leeway for nodes that override this default to a larger value.
const maxGrpcMsgSize = 32 * 1024 * 1024

func run(c *cli.Context) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	confDir := c.String("configdir")
	err := os.MkdirAll(confDir, os.ModePerm)
	if err != nil {
		return err
	}
	dbPath := filepath.Join(confDir, dbFn)

	log.Infow("Circuit Breaker starting", "version", BuildVersion)

	log.Infow("Opening database", "path", dbPath)

	// Open database.
	db, err := NewDb(ctx, dbPath, c.Int("fwdhistorylimit"))
	if err != nil {
		return err
	}
	defer func() {
		err := db.Close()
		if err != nil {
			log.Errorw("Error closing db", "err", err)
		}
	}()

	group, ctx := errgroup.WithContext(ctx)

	stub := c.Bool(stubFlag.Name)
	var client lndclient
	if stub {
		stubClient := newStubClient(ctx)

		client = stubClient
	} else {
		// First, we'll parse the args from the command.
		tlsCertPath, macPath, err := extractPathArgs(c)
		if err != nil {
			return err
		}

		lndCfg := LndConfig{
			RpcServer:   c.GlobalString("rpcserver"),
			TlsCertPath: tlsCertPath,
			MacPath:     macPath,
			Log:         log,
		}

		lndClient, err := NewLndClient(&lndCfg)
		if err != nil {
			return err
		}
		defer lndClient.Close()

		client = lndClient
	}

	// Load historical forwards if found.
	loadHist := c.String("loadhist")
	if loadHist != "" {
		info, err := client.getInfo()
		if err != nil {
			return err
		}

		if err = loadHistoricalForwards(
			ctx, loadHist, db, info.alias,
		); err != nil {
			return err
		}
	} else {
		log.Infof("No htlc_forwards.csv file to import: %v", loadHist)
	}

	limits, err := db.GetLimits(ctx)
	if err != nil {
		return err
	}

	p, err := NewProcess(client, log, limits, db)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxGrpcMsgSize),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer()),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer()),
	)

	reflection.Register(grpcServer)

	server := NewServer(log, p, client, db)

	circuitbreakerrpc.RegisterServiceServer(
		grpcServer, server,
	)

	listenAddress := c.String("listen")
	grpcInternalListener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		return err
	}

	// Create a client connection to the gRPC server we just started
	// This is where the gRPC-Gateway proxies the requests
	conn, err := grpc.DialContext(
		ctx,
		listenAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxGrpcMsgSize),
		),
	)
	if err != nil {
		return err
	}

	// Create http server.
	gwmux := runtime.NewServeMux()

	err = circuitbreakerrpc.RegisterServiceHandler(ctx, gwmux, conn)
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.Handle("/api/", http.StripPrefix("/api", gwmux))

	httpListen := c.String(httpListenFlag.Name)
	gwServer := &http.Server{
		Addr:              httpListen,
		Handler:           mux,
		ReadHeaderTimeout: time.Second * 10,
	}

	// Run circuitbreaker core.
	group.Go(func() error {
		return p.Run(ctx)
	})

	// Run grpc server.
	group.Go(func() error {
		log.Infow("Grpc server starting", "listenAddress", listenAddress)
		err := grpcServer.Serve(grpcInternalListener)
		if err != nil && err != grpc.ErrServerStopped {
			log.Errorw("grpc server error", "err", err)
		}

		return err
	})

	// Run http server.
	group.Go(func() error {
		log.Infow("HTTP server starting", "listenAddress", httpListen)

		return gwServer.ListenAndServe()
	})

	// Stop servers when context is cancelled.
	group.Go(func() error {
		<-ctx.Done()

		// Stop http server.
		log.Infof("Stopping http server")
		err := gwServer.Shutdown(context.Background()) //nolint:contextcheck
		if err != nil {
			log.Errorw("Error shutting down http server", "err", err)
		}

		// Stop grpc server.
		log.Infof("Stopping grpc server")
		grpcServer.Stop()

		return nil
	})

	group.Go(func() error {
		log.Infof("Press ctrl-c to exit")

		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

		select {
		case <-sigint:
			return errUserExit

		case <-ctx.Done():
			return nil
		}
	})

	return group.Wait()
}

// read sim-ln generated CSV file of historical forwards into db.
func loadHistoricalForwards(ctx context.Context, path string, db *Db,
	alias string) error {

	log.Infof("Loading historical forwards from: %v.", path)

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	rows, err := csv.NewReader(file).ReadAll()
	if err != nil {
		return err
	}

	// We're loading in historical HTLCs, but we also want to allow
	// regular operation of the channel (and we require unique index for
	// our db) so we start with an index that we won't hit for real
	// forwards once circuitbreaker starts running.
	var startIdx uint64 = math.MaxUint64 / 4
	var ourHTLCs int
	for i, record := range rows {
		recordAlias := record[9]
		if recordAlias != alias {
			continue
		}

		addTimeUnix, err := strconv.ParseInt(record[2], 10, 64)
		if err != nil {
			return fmt.Errorf("add time: %w", err)
		}

		resolveTimeUnix, err := strconv.ParseInt(record[3], 10, 64)
		if err != nil {
			return fmt.Errorf("remove time: %w", err)
		}

		amountIn, err := strconv.ParseInt(record[0], 10, 64)
		if err != nil {
			return fmt.Errorf("amount in: %w", err)
		}

		amountOut, err := strconv.ParseInt(record[4], 10, 64)
		if err != nil {
			return fmt.Errorf("amount out: %w", err)
		}

		cltvIn, err := strconv.ParseInt(record[1], 10, 64)
		if err != nil {
			return fmt.Errorf("cltv in: %w", err)
		}

		cltvOut, err := strconv.ParseInt(record[5], 10, 64)
		if err != nil {
			return fmt.Errorf("cltv out: %v", err)
		}

		if cltvIn < cltvOut {
			return fmt.Errorf("cltv in %v < cltv out %v", cltvIn, cltvOut)
		}

		incomingChannel, err := strconv.ParseInt(record[10], 10, 64)
		if err != nil {
			return fmt.Errorf("incoming channel: %w", err)
		}

		outgoingChannel, err := strconv.ParseInt(record[11], 10, 64)
		if err != nil {
			return fmt.Errorf("outgoing channel: %w", err)
		}

		htlcInfo := &HtlcInfo{
			addTime:      time.Unix(0, addTimeUnix),
			resolveTime:  time.Unix(0, resolveTimeUnix),
			settled:      true, // note: hard coding rn
			incomingMsat: lnwire.MilliSatoshi(amountIn),
			outgoingMsat: lnwire.MilliSatoshi(amountOut),
			// Note: we don't provide incoming/outgoing peer rn.
			incomingCircuit: circuitKey{
				channel: uint64(incomingChannel),
				htlc:    startIdx + uint64(i),
			},
			outgoingCircuit: circuitKey{
				channel: uint64(outgoingChannel),
				htlc:    startIdx + uint64(i),
			},
			incomingEndorsed: lrc.Endorsement(1),
			outgoingEndorsed: lrc.Endorsement(0),
			cltvDelta:        uint32(cltvIn - cltvOut),
		}

		// Track imported HTLC count.
		ourHTLCs++

		// Append the HtlcInfo to the slice
		if err := db.insertHtlcResolution(ctx, htlcInfo); err != nil {
			return err
		}
	}

	log.Infof("Successfully imported: %v htlcs for node alias: %v.", ourHTLCs, alias)

	return nil
}
