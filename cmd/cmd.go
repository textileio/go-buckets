package cmd

import (
	"time"

	"github.com/manifoldco/promptui"
	"github.com/spf13/viper"
)

var (
	// Timeout is the default timeout used for most commands.
	Timeout = time.Minute * 10
	// PushTimeout is the command timeout used when pushing bucket changes.
	PushTimeout = time.Hour * 24
	// PullTimeout is the command timeout used when pulling bucket changes.
	PullTimeout = time.Hour * 24

	// Bold is a styler used to make the output text bold.
	Bold = promptui.Styler(promptui.FGBold)
)

// Flag describes a command flag.
type Flag struct {
	Key      string
	DefValue interface{}
}

// Config describes a command config params and file info.
type Config struct {
	Viper  *viper.Viper
	File   string
	Dir    string
	Name   string
	Flags  map[string]Flag
	EnvPre string
	Global bool
}

// ConfConfig is used to generate new messages configs.
type ConfConfig struct {
	Dir       string // Config directory base name
	Name      string // Name of the mailbox config file
	Type      string // Type is the type of config file (yaml/json)
	EnvPrefix string // A prefix that will be expected on env vars
}

// NewConfig uses values from ConfConfig to contruct a new config.
func (cc ConfConfig) NewConfig(pth string, flags map[string]Flag, global bool) (c *Config, fileExists bool, err error) {
	v := viper.New()
	v.SetConfigType(cc.Type)
	c = &Config{
		Viper:  v,
		Dir:    cc.Dir,
		Name:   cc.Name,
		Flags:  flags,
		EnvPre: cc.EnvPrefix,
		Global: global,
	}
	fileExists = FindConfigFile(c, pth)
	return c, fileExists, nil
}
