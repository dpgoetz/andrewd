//  Copyright (c) 2016 Rackspace
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
//  implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package andrewd

import (
	"flag"
	"fmt"
	"log/syslog"
	"os"
	"os/signal"
	"syscall"

	"github.com/troubling/hummingbird/common/conf"
)

type LowLevelLogger interface {
	Err(string) error
	Info(string) error
	Debug(string) error
}

type consoleLogger struct{}

func (c *consoleLogger) Err(m string) error {
	fmt.Println("ERROR:", m)
	return nil
}

func (c *consoleLogger) Info(m string) error {
	fmt.Println("INFO:", m)
	return nil
}

func (c *consoleLogger) Debug(m string) error {
	fmt.Println("DEBUG:", m)
	return nil
}

var syslogFacilityMapping = map[string]syslog.Priority{"LOG_USER": syslog.LOG_USER,
	"LOG_MAIL": syslog.LOG_MAIL, "LOG_DAEMON": syslog.LOG_DAEMON,
	"LOG_AUTH": syslog.LOG_AUTH, "LOG_SYSLOG": syslog.LOG_SYSLOG,
	"LOG_LPR": syslog.LOG_LPR, "LOG_NEWS": syslog.LOG_NEWS,
	"LOG_UUCP": syslog.LOG_UUCP, "LOG_CRON": syslog.LOG_CRON,
	"LOG_AUTHPRIV": syslog.LOG_AUTHPRIV, "LOG_FTP": syslog.LOG_FTP,
	"LOG_LOCAL0": syslog.LOG_LOCAL0, "LOG_LOCAL1": syslog.LOG_LOCAL1,
	"LOG_LOCAL2": syslog.LOG_LOCAL2, "LOG_LOCAL3": syslog.LOG_LOCAL3,
	"LOG_LOCAL4": syslog.LOG_LOCAL4, "LOG_LOCAL5": syslog.LOG_LOCAL5,
	"LOG_LOCAL6": syslog.LOG_LOCAL6, "LOG_LOCAL7": syslog.LOG_LOCAL7}

// SetupLogger pulls configuration information from the config and flags to create a UDP syslog logger.
// If -d was not specified, it also logs to the console.
func SetupLogger(conf conf.Config, flags *flag.FlagSet, section, prefix string) (LowLevelLogger, error) {
	vFlag := flags.Lookup("v")
	dFlag := flags.Lookup("d")
	if vFlag != nil && dFlag != nil && vFlag.Value.(flag.Getter).Get().(bool) && !dFlag.Value.(flag.Getter).Get().(bool) {
		return &consoleLogger{}, nil
	}
	facility := conf.GetDefault(section, "log_facility", "LOG_LOCAL0")
	host := conf.GetDefault(section, "log_udp_host", "127.0.0.1")
	port := conf.GetInt(section, "log_udp_port", 514)
	dialHost := fmt.Sprintf("%s:%d", host, port)
	logger, err := syslog.Dial("udp", dialHost, syslogFacilityMapping[facility], prefix)
	if err != nil {
		return nil, fmt.Errorf("Unable to dial logger: %v", err)
	}
	return logger, nil
}
func ShutdownStdio() {
	devnull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0600)
	if err != nil {
		panic("Error opening /dev/null")
	}
	syscall.Dup2(int(devnull.Fd()), int(os.Stdin.Fd()))
	syscall.Dup2(int(devnull.Fd()), int(os.Stdout.Fd()))
	syscall.Dup2(int(devnull.Fd()), int(os.Stderr.Fd()))
	devnull.Close()
}

type Daemon interface {
	Run()
	RunForever()
	LogError(format string, args ...interface{})
}

func RunDaemon(GetDaemon func(conf.Config, *flag.FlagSet) (Daemon, error), flags *flag.FlagSet) {

	if flags.NArg() != 0 {
		flags.Usage()
		return
	}

	configFile := flags.Lookup("c").Value.(flag.Getter).Get().(string)
	config, err := conf.LoadConfig(configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error finding config: %v\n", err)
		return
	}

	once := flags.Lookup("once").Value.(flag.Getter).Get() == true

	if daemon, err := GetDaemon(config, flags); err == nil {
		if once {
			daemon.Run()
			fmt.Fprintf(os.Stderr, "Daemon pass completed.\n")
			daemon.LogError("Daemon pass completed.")
		} else {
			go daemon.RunForever()
			fmt.Fprintf(os.Stderr, "Daemon started.\n")
			daemon.LogError("Daemon started.")
		}
		if flags.Lookup("d").Value.(flag.Getter).Get() == true {
			ShutdownStdio()
		}
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
		<-c
	} else {
		fmt.Fprintf(os.Stderr, "Failed to create daemon: %v", err)
	}

}
