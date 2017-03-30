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
	"net"
	"net/http"
	"time"

	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/srv"
)

var Account = ".dispersion"
var Container = "dObjects"

type DispersionMonitor struct {
	oring  ring.Ring
	logger srv.LowLevelLogger
}

func (dm *DispersionMonitor) getDispersionObjects(objNames chan string) {
	defer close(objNames)
	for partition := uint64(0); true; partition++ {
		devs := dm.oring.GetNodesInOrder(partition)
		if devs == nil {
			break
		}
		for i := uint64(0); true; i++ {
			obj := fmt.Sprintf("%d-%d", partition, i)
			genPart := dm.oring.GetPartition(Account, Container, obj)
			if genPart == partition {
				objNames <- obj
				break
			}
		}
	}
}

func (dm *DispersionMonitor) putDispersionObjects(objNames []string) bool {
	client := http.Client{ // TODO: make this a class lvl client
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 5 * time.Second,
			}).Dial,
		},
		Timeout: 20 * time.Second,
	}
	successes := uint64(0)
	numReplicas := dm.oring.ReplicaCount()
	for _, obj := range objNames {
		partition := dm.oring.GetPartition(Account, Container, obj)
		fmt.Println("000")

		for _, device := range dm.oring.GetNodes(partition) {
			for retry := uint64(0); retry < 3; retry++ {
				url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s",
					device.Ip, device.Port, device.Device,
					partition, Account, Container, obj)
				fmt.Println(fmt.Sprintf("aaa: %s", url))
				req, _ := http.NewRequest("PUT", url, nil)
				req.Header.Set("Content-Length", "0")
				req.Header.Set("Content-Type", "text")
				req.Header.Set("X-Timestamp", fmt.Sprintf("%d", time.Now().Unix()))

				resp, err := client.Do(req)
				if err != nil {
					fmt.Println("111")
					fmt.Println(fmt.Sprintf("Error on PUT to %s: %v\n", url, err))
					continue
				}
				resp.Body.Close()
				if resp.StatusCode/100 != 2 {
					fmt.Println("222")
					time.Sleep(2 << retry * time.Second)
					fmt.Println(fmt.Sprintf("PUT to %s got %d", url, resp.StatusCode))
				} else {
					fmt.Println("333")
					successes += 1
					break
				}
			}
		}
	}
	totalObjects := uint64(len(objNames)) * numReplicas
	fmt.Println(fmt.Sprintf("successes: %d, totelObj: %d, numRep: %d, %d", successes, totalObjects, numReplicas, len(objNames)))
	success := successes == totalObjects
	if success {
		fmt.Println("All Dispersion Objects PUT successfully!!")
	} else {
		fmt.Println(fmt.Sprintf("Missing %d Dispersion Objects.",
			totalObjects-successes))
	}
	return success
}

func (dm *DispersionMonitor) LogError(format string, args ...interface{}) {
	dm.logger.Err(fmt.Sprintf(format, args...))
}

func (dm *DispersionMonitor) LogDebug(format string, args ...interface{}) {
	dm.logger.Debug(fmt.Sprintf(format, args...))
}

func (dm *DispersionMonitor) LogInfo(format string, args ...interface{}) {
	dm.logger.Info(fmt.Sprintf(format, args...))
}

func (dm *DispersionMonitor) Run() {

}

func (dm *DispersionMonitor) RunForever() {
	dm.LogInfo("Andrewd Disperion Monitor Starting Run")

}

func GetDispersionMonitor(serverconf conf.Config, flags *flag.FlagSet) (srv.Daemon, error) {

	hashPathPrefix, hashPathSuffix, err := conf.GetHashPrefixAndSuffix()
	if err != nil {
		fmt.Println("Unable to load hash path prefix and suffix:", err)
		return nil, err
	}
	objRing, err := ring.GetRing("object", hashPathPrefix, hashPathSuffix, 0)
	if err != nil {
		fmt.Println("Unable to load ring:", err)
		return nil, err
	}

	logger, err := srv.SetupLogger(serverconf, flags, "andrewd", "dispersion")

	dm := DispersionMonitor{
		oring:  objRing,
		logger: logger,
	}
	return &dm, nil
}
