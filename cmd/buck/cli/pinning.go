package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/spf13/cobra"
	"github.com/textileio/go-buckets/cmd"
)

var pinningCmd = &cobra.Command{
	Use:   "pinning",
	Short: "Manage IPFS pinning service",
	Long:  `Enable/disable bucket as an IPFS Pinning Service.`,
	Args:  cobra.ExactArgs(0),
}

var pinningEnableCmd = &cobra.Command{
	Use:   "enable",
	Short: "Enable bucket as an IPFS Pinning Service",
	Long:  `Enables the bucket as an IPFS Pinning Service for a locally running IPFS node.`,
	Args:  cobra.ExactArgs(0),
	Run: func(c *cobra.Command, args []string) {
		conf, err := bucks.NewConfigFromCmd(c, ".")
		cmd.ErrCheck(err)
		ctx, cancel := context.WithTimeout(context.Background(), cmd.Timeout)
		defer cancel()
		buck, err := bucks.GetLocalBucket(ctx, conf)
		cmd.ErrCheck(err)

		token, err := buck.GetIdentityToken(time.Hour * 24 * 30)
		cmd.ErrCheck(err)

		links, err := buck.RemoteLinks(ctx, "")
		cmd.ErrCheck(err)

		doExec(
			fmt.Sprintf("ipfs pin remote service add %s %s %s", buck.Key(), links.BPS, string(token)),
			nil)

		cmd.Success("Enabled bucket as pinning service: %s", aurora.White(buck.Key()).Bold())
	},
}

var pinningDisableCmd = &cobra.Command{
	Use:   "disable",
	Short: "Disable bucket as an IPFS Pinning Service",
	Long:  `Disables the bucket as an IPFS Pinning Service for a locally running IPFS node.`,
	Args:  cobra.ExactArgs(0),
	Run: func(c *cobra.Command, args []string) {
		conf, err := bucks.NewConfigFromCmd(c, ".")
		cmd.ErrCheck(err)
		ctx, cancel := context.WithTimeout(context.Background(), cmd.Timeout)
		defer cancel()
		buck, err := bucks.GetLocalBucket(ctx, conf)
		cmd.ErrCheck(err)

		doExec(fmt.Sprintf("ipfs pin remote service rm %s", buck.Key()), nil)

		cmd.Success("Disabled bucket as pinning service")
	},
}

var pinningAddCmd = &cobra.Command{
	Use:   "add [path]",
	Short: "Add path to local IPFS node and pin to bucket",
	Long:  `Adds the path to the locally running IPFS node and pins the resulting CID to the bucket`,
	Args:  cobra.ExactArgs(1),
	Run: func(c *cobra.Command, args []string) {
		name, err := c.Flags().GetString("name")
		cmd.ErrCheck(err)

		conf, err := bucks.NewConfigFromCmd(c, ".")
		cmd.ErrCheck(err)
		ctx, cancel := context.WithTimeout(context.Background(), cmd.Timeout)
		defer cancel()
		buck, err := bucks.GetLocalBucket(ctx, conf)
		cmd.ErrCheck(err)

		var buf bytes.Buffer
		doExec(fmt.Sprintf("ipfs add -Qr --cid-version=1 %s", args[0]), &buf)

		doExec(
			fmt.Sprintf("ipfs pin remote add --background --service=%s --name=%s %s",
				buck.Key(),
				name,
				buf.String(),
			),
			os.Stdout,
		)
	},
}

var pinningRmCmd = &cobra.Command{
	Use:   "rm [name|CID]",
	Short: "Remove pin by name or CID from bucket",
	Long:  "Removes a pin by name or CID from the bucket.",
	Args:  cobra.ExactArgs(1),
	Run: func(c *cobra.Command, args []string) {
		conf, err := bucks.NewConfigFromCmd(c, ".")
		cmd.ErrCheck(err)
		ctx, cancel := context.WithTimeout(context.Background(), cmd.Timeout)
		defer cancel()
		buck, err := bucks.GetLocalBucket(ctx, conf)
		cmd.ErrCheck(err)

		var match string
		if _, err := cid.Decode(args[0]); err == nil {
			match = fmt.Sprintf("--cid=%s", args[0])
		} else {
			match = fmt.Sprintf("--name=%s", args[0])
		}

		doExec(
			fmt.Sprintf(
				"ipfs pin remote rm --service=%s --status=queued,pinning,pinned,failed %s",
				buck.Key(),
				match,
			),
			os.Stdout,
		)
	},
}

var pinningLsCmd = &cobra.Command{
	Use:   "ls",
	Short: "Disable bucket as an IPFS Pinning Service",
	Long:  `Disables the bucket as an IPFS Pinning Service for a locally running IPFS node.`,
	Args:  cobra.ExactArgs(0),
	Run: func(c *cobra.Command, args []string) {
		conf, err := bucks.NewConfigFromCmd(c, ".")
		cmd.ErrCheck(err)
		ctx, cancel := context.WithTimeout(context.Background(), cmd.Timeout)
		defer cancel()
		buck, err := bucks.GetLocalBucket(ctx, conf)
		cmd.ErrCheck(err)

		doExec(
			fmt.Sprintf("ipfs pin remote ls --service=%s --status=queued,pinning,pinned,failed", buck.Key()),
			os.Stdout)
	},
}

func doExec(c string, out io.Writer) {
	var com *exec.Cmd
	if runtime.GOOS == "windows" {
		com = exec.Command("cmd", "/C", c)
	} else {
		com = exec.Command("bash", "-c", c)
	}
	if out != nil {
		com.Stdout = out
	}
	err := com.Run()
	cmd.ErrCheck(err)
}
