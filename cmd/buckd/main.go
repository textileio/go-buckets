package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	ds "github.com/ipfs/go-datastore"
	badger "github.com/ipfs/go-ds-badger"
	httpapi "github.com/ipfs/go-ipfs-http-client"
	logging "github.com/ipfs/go-log/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/textileio/go-buckets"
	"github.com/textileio/go-buckets/api/common"
	"github.com/textileio/go-buckets/cmd"
	dns "github.com/textileio/go-buckets/dns"
	"github.com/textileio/go-buckets/gateway"
	ipns "github.com/textileio/go-buckets/ipns"
	mongods "github.com/textileio/go-ds-mongo"
	dbc "github.com/textileio/go-threads/api/client"
	"github.com/textileio/go-threads/core/did"
	nc "github.com/textileio/go-threads/net/api/client"
	"github.com/textileio/go-threads/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const daemonName = "buckd"

var (
	log = logging.Logger(daemonName)

	config = &cmd.Config{
		Viper: viper.New(),
		Dir:   "." + daemonName,
		Name:  "config",
		Flags: map[string]cmd.Flag{
			"debug": {
				Key:      "log.debug",
				DefValue: false,
			},
			"logFile": {
				Key:      "log.file",
				DefValue: "", // no log file
			},

			// Addresses
			"addrApi": {
				Key:      "addr.api",
				DefValue: "127.0.0.1:5000",
			},
			"addrApiProxy": {
				Key:      "addr.api_proxy",
				DefValue: "127.0.0.1:5050",
			},
			"addrGateway": {
				Key:      "addr.gateway",
				DefValue: "127.0.0.1:8000",
			},

			// Datastore
			"datastoreType": {
				Key:      "datastore.type",
				DefValue: "badger",
			},
			"datastoreBadgerRepo": {
				Key:      "datastore.badger.repo",
				DefValue: "${HOME}/." + daemonName + "/repo",
			},
			"datastoreMongoUri": {
				Key:      "datastore.mongo.uri",
				DefValue: "mongodb://127.0.0.1:27017",
			},
			"datastoreMongoName": {
				Key:      "datastore.mongo.name",
				DefValue: "buckets",
			},

			// Gateway
			"gatewayUrl": {
				Key:      "gateway.url",
				DefValue: "http://127.0.0.1:8000",
			},
			"gatewaySubdomains": {
				Key:      "gateway.subdomains",
				DefValue: false,
			},
			"gatewayWwwDomain": {
				Key:      "gateway.www_domain",
				DefValue: "",
			},

			// Threads
			"threadsAddr": {
				Key:      "threads.addr",
				DefValue: "127.0.0.1:4000",
			},

			// IPFS
			"ipfsMultiaddr": {
				Key:      "ipfs.multiaddr",
				DefValue: "/ip4/127.0.0.1/tcp/5001",
			},

			// IPNS
			"ipnsRepublishSchedule": {
				Key:      "ipns.republish_schedule",
				DefValue: "0 1 * * *",
			},
			"ipnsRepublishConcurrency": {
				Key:      "ipns.republish_concurrency",
				DefValue: 100,
			},

			// Cloudflare
			"cloudflareDnsZoneID": {
				Key:      "cloudflare.dns.zone_id",
				DefValue: "",
			},
			"cloudflareDnsToken": {
				Key:      "cloudflare.dns.token",
				DefValue: "",
			},
		},
		EnvPre: "BUCK",
		Global: true,
	}
)

func init() {
	cobra.OnInitialize(cmd.InitConfig(config))
	cmd.InitConfigCmd(rootCmd, config.Viper, config.Dir)

	rootCmd.PersistentFlags().StringVar(
		&config.File,
		"config",
		"",
		"Config file (default ${HOME}/"+config.Dir+"/"+config.Name+".yml)")

	rootCmd.PersistentFlags().BoolP(
		"debug",
		"d",
		config.Flags["debug"].DefValue.(bool),
		"Enable debug logging")
	rootCmd.PersistentFlags().String(
		"logFile",
		config.Flags["logFile"].DefValue.(string),
		"Write logs to file")

	// Addresses
	rootCmd.PersistentFlags().String(
		"addrApi",
		config.Flags["addrApi"].DefValue.(string),
		"API listen address")
	rootCmd.PersistentFlags().String(
		"addrApiProxy",
		config.Flags["addrApiProxy"].DefValue.(string),
		"API proxy listen address")
	rootCmd.PersistentFlags().String(
		"addrGateway",
		config.Flags["addrGateway"].DefValue.(string),
		"Gateway listen address")

	// Datastore
	rootCmd.PersistentFlags().String(
		"datastoreType",
		config.Flags["datastoreType"].DefValue.(string),
		"Datastore type (badger/mongo)")
	rootCmd.PersistentFlags().String(
		"datastoreBadgerRepo",
		config.Flags["datastoreBadgerRepo"].DefValue.(string),
		"Path to badger repository")
	rootCmd.PersistentFlags().String(
		"datastoreMongoUri",
		config.Flags["datastoreMongoUri"].DefValue.(string),
		"MongoDB connection URI")
	rootCmd.PersistentFlags().String(
		"datastoreMongoName",
		config.Flags["datastoreMongoName"].DefValue.(string),
		"MongoDB database name")

	// Gateway
	rootCmd.PersistentFlags().String(
		"gatewayUrl",
		config.Flags["gatewayUrl"].DefValue.(string),
		"Gateway URL")
	rootCmd.PersistentFlags().Bool(
		"gatewaySubdomains",
		config.Flags["gatewaySubdomains"].DefValue.(bool),
		"Enable gateway namespace subdomain redirection")
	rootCmd.PersistentFlags().String(
		"gatewayWwwDomain",
		config.Flags["gatewayWwwDomain"].DefValue.(string),
		"Bucket website domain")

	// Threads
	rootCmd.PersistentFlags().String(
		"threadsAddr",
		config.Flags["threadsAddr"].DefValue.(string),
		"Threads API address")

	// IPFS
	rootCmd.PersistentFlags().String(
		"ipfsMultiaddr",
		config.Flags["ipfsMultiaddr"].DefValue.(string),
		"IPFS API multiaddress")

	// IPNS
	rootCmd.PersistentFlags().String(
		"ipnsRepublishSchedule",
		config.Flags["ipnsRepublishSchedule"].DefValue.(string),
		"IPNS republishing cron schedule")
	rootCmd.PersistentFlags().Int(
		"ipnsRepublishConcurrency",
		config.Flags["ipnsRepublishConcurrency"].DefValue.(int),
		"IPNS republishing batch size")

	// Cloudflare
	rootCmd.PersistentFlags().String(
		"cloudflareDnsZoneID",
		config.Flags["cloudflareDnsZoneID"].DefValue.(string),
		"Cloudflare ZoneID for dnsDomain")
	rootCmd.PersistentFlags().String(
		"cloudflareDnsToken",
		config.Flags["cloudflareDnsToken"].DefValue.(string),
		"Cloudflare API Token for dnsDomain")

	err := cmd.BindFlags(config.Viper, rootCmd, config.Flags)
	cmd.ErrCheck(err)
}

func main() {
	cmd.ErrCheck(rootCmd.Execute())
}

var rootCmd = &cobra.Command{
	Use:   daemonName,
	Short: "Buckets daemon",
	Long:  `The Buckets daemon.`,
	PersistentPreRun: func(c *cobra.Command, args []string) {
		config.Viper.SetConfigType("yaml")
		cmd.ExpandConfigVars(config.Viper, config.Flags)

		if config.Viper.GetBool("log.debug") {
			err := util.SetLogLevels(map[string]logging.LogLevel{
				daemonName:        logging.LevelDebug,
				"buckets":         logging.LevelDebug,
				"buckets-api":     logging.LevelDebug,
				"buckets-gateway": logging.LevelDebug,
				"buckets-ipns":    logging.LevelDebug,
				"buckets-dns":     logging.LevelDebug,
			})
			cmd.ErrCheck(err)
		}
	},
	Run: func(c *cobra.Command, args []string) {
		settings, err := json.MarshalIndent(config.Viper.AllSettings(), "", "  ")
		cmd.ErrCheck(err)
		log.Debugf("loaded config: %s", string(settings))

		logFile := config.Viper.GetString("log.file")
		if logFile != "" {
			err = cmd.SetupDefaultLoggingConfig(logFile)
			cmd.ErrCheck(err)
		}

		addrApi := config.Viper.GetString("addr.api")
		addrApiProxy := config.Viper.GetString("addr.api_proxy")
		addrGateway := config.Viper.GetString("addr.gateway")

		datastoreType := config.Viper.GetString("datastore.type")
		datastoreBadgerRepo := config.Viper.GetString("datastore.badger.repo")
		datastoreMongoUri := config.Viper.GetString("datastore.mongo.uri")
		datastoreMongoName := config.Viper.GetString("datastore.mongo.name")

		gatewayUrl := config.Viper.GetString("gateway.url")
		gatewaySubdomains := config.Viper.GetBool("gateway.subdomains")
		gatewayWwwDomain := config.Viper.GetString("gateway.www_domain")

		threadsApi := config.Viper.GetString("threads.addr")
		ipfsApi := cmd.AddrFromStr(config.Viper.GetString("ipfs.multiaddr"))

		//ipnsRepublishSchedule := config.Viper.GetString("ipns.republish_schedule")
		//ipnsRepublishConcurrency := config.Viper.GetInt("ipns.republish_concurrency")

		cloudflareDnsZoneID := config.Viper.GetString("cloudflare.dns.zone_id")
		cloudflareDnsToken := config.Viper.GetString("cloudflare.dns.token")

		net, err := nc.NewClient(threadsApi, getClientRPCOpts(threadsApi)...)
		cmd.ErrCheck(err)
		db, err := dbc.NewClient(threadsApi, getClientRPCOpts(threadsApi)...)
		cmd.ErrCheck(err)
		ipfs, err := httpapi.NewApi(ipfsApi)
		cmd.ErrCheck(err)

		var ipnsms ds.TxnDatastore
		switch datastoreType {
		case "badger":
			ipnsms, err = newBadgerStore(datastoreBadgerRepo)
			cmd.ErrCheck(err)
		case "mongo":
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ipnsms, err = newMongoStore(ctx, datastoreMongoUri, datastoreMongoName, "ipns")
			cmd.ErrCheck(err)
		default:
			cmd.Fatal(errors.New("datastoreType must be 'badger' or 'mongo'"))
		}
		ipnsm, err := ipns.NewManager(ipnsms, ipfs)
		cmd.ErrCheck(err)

		var dnsm *dns.Manager
		if len(cloudflareDnsZoneID) != 0 && len(cloudflareDnsToken) != 0 {
			dnsm, err = dns.NewManager(gatewayWwwDomain, cloudflareDnsZoneID, cloudflareDnsToken)
			cmd.ErrCheck(err)
		} else if len(cloudflareDnsZoneID) != 0 || len(cloudflareDnsToken) != 0 {
			cmd.Fatal(errors.New("cloudflareDnsZoneID or cloudflareDnsToken not specified"))
		}

		lib, err := buckets.NewBuckets(net, db, ipfs, ipnsm, dnsm)
		cmd.ErrCheck(err)

		buckets.GatewayURL = gatewayUrl
		buckets.WWWDomain = gatewayWwwDomain

		server, proxy, err := common.GetServerAndProxy(lib, addrApi, addrApiProxy)
		cmd.ErrCheck(err)

		// Configure gateway
		gateway, err := gateway.NewGateway(lib, ipfs, ipnsm, gateway.Config{
			Addr:       addrGateway,
			URL:        gatewayUrl,
			Domain:     gatewayWwwDomain,
			Subdomains: gatewaySubdomains,
		})
		cmd.ErrCheck(err)
		gateway.Start()

		fmt.Println("Welcome to Buckets!")

		cmd.HandleInterrupt(func() {
			err := gateway.Close()
			cmd.LogErr(err)
			log.Info("gateway was shutdown")

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err = proxy.Shutdown(ctx)
			cmd.LogErr(err)
			log.Info("proxy was shutdown")

			stopped := make(chan struct{})
			go func() {
				server.GracefulStop()
				close(stopped)
			}()
			timer := time.NewTimer(10 * time.Second)
			select {
			case <-timer.C:
				server.Stop()
			case <-stopped:
				timer.Stop()
			}
			log.Info("server was shutdown")

			err = lib.Close()
			cmd.LogErr(err)
			log.Info("bucket lib was shutdown")

			err = ipnsm.Close()
			cmd.LogErr(err)
			log.Info("ipns manager was shutdown")

			err = net.Close()
			cmd.LogErr(err)
			log.Info("net client was shutdown")
		})
	},
}

func getClientRPCOpts(target string) (opts []grpc.DialOption) {
	creds := did.RPCCredentials{}
	if strings.Contains(target, "443") {
		tcreds := credentials.NewTLS(&tls.Config{})
		opts = append(opts, grpc.WithTransportCredentials(tcreds))
		creds.Secure = true
	} else {
		opts = append(opts, grpc.WithInsecure())
	}
	opts = append(opts, grpc.WithPerRPCCredentials(creds))
	return opts
}

func newBadgerStore(repo string) (ds.TxnDatastore, error) {
	if err := os.MkdirAll(repo, os.ModePerm); err != nil {
		return nil, err
	}
	return badger.NewDatastore(repo, &badger.DefaultOptions)
}

func newMongoStore(ctx context.Context, uri, db, collection string) (ds.TxnDatastore, error) {
	return mongods.New(ctx, uri, db, mongods.WithCollName(collection))
}
