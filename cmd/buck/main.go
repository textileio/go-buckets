package main

import (
	"github.com/spf13/cobra"
	bc "github.com/textileio/go-buckets/api/client"
	"github.com/textileio/go-buckets/api/common"
	"github.com/textileio/go-buckets/cmd"
	buck "github.com/textileio/go-buckets/cmd/buck/cli"
	"github.com/textileio/go-buckets/local"
)

const defaultTarget = "127.0.0.1:5000"

var client *bc.Client

func init() {
	buck.Init(rootCmd)

	rootCmd.PersistentFlags().String("api", defaultTarget, "API target")
}

func main() {
	cmd.ErrCheck(rootCmd.Execute())
}

var rootCmd = &cobra.Command{
	Use:   buck.Name,
	Short: "Bucket Client",
	Long: `The Bucket Client.

Manages files and folders in an object storage bucket.`,
	PersistentPreRun: func(c *cobra.Command, args []string) {
		config := local.DefaultConfConfig()
		target := cmd.GetFlagOrEnvValue(c, "api", config.EnvPrefix)

		var err error
		client, err = bc.NewClient(target, common.GetClientRPCOpts(target)...)
		cmd.ErrCheck(err)
		buck.SetBucks(local.NewBuckets(client, config))
	},
	PersistentPostRun: func(c *cobra.Command, args []string) {
		cmd.ErrCheck(client.Close())
	},
	Args: cobra.ExactArgs(0),
}
