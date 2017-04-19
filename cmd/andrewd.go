//  Copyright (c) 2015 Rackspace
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

package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"

	"github.com/dpgoetz/andrewd"
	"github.com/troubling/hummingbird/client"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/srv"
)

var Version = "0.1"
var PidDir = "/var/run/hummingbird"
var PidLoc = "/var/run/hummingbird/andrewd.pid"

func WritePid(pid int) error {
	file, err := os.Create(PidLoc)
	if err != nil {
		return err
	}
	fmt.Fprintf(file, "%d", pid)
	file.Close()
	return nil
}

func RemovePid() error {
	return os.RemoveAll(PidLoc)
}

func GetProcess() (*os.Process, error) {
	var pid int
	file, err := os.Open(PidLoc)
	if err != nil {
		return nil, err
	}
	_, err = fmt.Fscanf(file, "%d", &pid)
	if err != nil {
		return nil, err
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		return nil, err
	}
	err = process.Signal(syscall.Signal(0))
	if err != nil {
		return nil, err
	}
	return process, nil
}

func getConfig() string {
	confLoc := "/etc/hummingbird/andrewd.conf"
	if _, err := os.Stat(confLoc); os.IsNotExist(err) {
		confLoc = "/etc/swift/andrewd.conf"
		if _, err := os.Stat(confLoc); os.IsNotExist(err) {
			return ""
		}
	}
	return confLoc
}

func StartDaemon() {
	_, err := GetProcess()
	if err == nil {
		return
	}
	confPath := getConfig()
	if confPath == "" {
		fmt.Println("Unable to find config file.")
		return
	}
	dExec, err := exec.LookPath(os.Args[0])
	if err != nil {
		fmt.Println("Unable to find executable in path.")
		return
	}
	uid, gid, err := conf.UidFromConf(confPath)
	cmd := exec.Command(dExec, "run", "-d", "-c", confPath)

	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if uint32(os.Getuid()) != uid {
		cmd.SysProcAttr.Credential = &syscall.Credential{Uid: uid, Gid: gid}
	}
	rdp, err := cmd.StdoutPipe()
	cmd.Stderr = cmd.Stdout
	if err != nil {
		fmt.Println("Error creating stdout pipe:", err)
		return
	}
	syscall.Umask(022)

	err = cmd.Start()
	if err != nil {
		fmt.Println("Error starting daemon: ", err)
		return
	}
	io.Copy(os.Stdout, rdp)
	if err = WritePid(cmd.Process.Pid); err != nil {
		fmt.Println("Could not create pid: ", err)
		cmd.Process.Signal(syscall.SIGTERM)
		cmd.Process.Wait()
	}
}

func StopDaemon() {
	process, err := GetProcess()
	if err != nil {
		fmt.Println("Error finding process: ", err)
		return
	}
	process.Signal(syscall.SIGTERM)
	process.Wait()
	RemovePid()
	fmt.Println("Process Stopped")
}

func RestartDaemon() {
	StopDaemon()
	StartDaemon()
}

func ProcessControlCommand(cmd func()) {
	if _, err := os.Stat(PidDir); os.IsNotExist(err) {
		err := os.MkdirAll(PidDir, 0600)
		if err != nil {
			fmt.Fprintf(os.Stderr, fmt.Sprintf("Unable to create %s\n", PidDir))
			fmt.Fprintf(os.Stderr, "You should create it, writable by the user you are running with.\n")
			os.Exit(1)
		}
	}
	cmd()
}

func main() {
	runFlags := flag.NewFlagSet("Run server", flag.ExitOnError)
	runFlags.Bool("d", false, "Close stdio once server is running")
	runFlags.Bool("once", false, "Run one pass of andrewd")
	runFlags.String("c", getConfig(), "Config file to use")
	runFlags.Bool("v", false, "Send all log messages to the console (if -d is not specified)")
	runFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, "andrewd run [ARGS]\n")
		runFlags.PrintDefaults()
	}
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: andrewd [command] [args...]\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "               start: start daemon\n")
		fmt.Fprintf(os.Stderr, "                stop: stop daemon\n")
		fmt.Fprintf(os.Stderr, "             restart: stop then restart daemon\n")
		fmt.Fprintf(os.Stderr, "             version: prints the version\n")
		fmt.Fprintf(os.Stderr, "                 run: run andrewd (attached)\n")
		fmt.Fprintf(os.Stderr, " populate-dispersion: populate 100%% dispersion objects in .dispersion/objs/ (takes a while)\n")
		fmt.Fprintf(os.Stderr, "\n")
		runFlags.Usage()
		fmt.Fprintf(os.Stderr, "\n")
	}
	flag.Parse()
	if flag.NArg() < 1 {
		flag.Usage()
		return
	}
	switch flag.Arg(0) {
	case "version":
		fmt.Println(Version)
	case "start":
		ProcessControlCommand(StartDaemon)
	case "stop":
		ProcessControlCommand(StopDaemon)
	case "restart":
		ProcessControlCommand(RestartDaemon)
	case "run":
		runFlags.Parse(flag.Args()[1:])
		srv.RunDaemon(andrewd.GetBirdCatcher, runFlags)
	case "populate-dispersion":
		runFlags.Parse(flag.Args()[1:])
		fmt.Println("Starting to put objects")

		hClient, err := client.NewProxyDirectClient()
		if err != nil {
			fmt.Println(fmt.Sprintf("Could not make client: %v", err))
		}
		andrewd.PutDispersionObjects(hClient)

	default:
		flag.Usage()
	}
}
