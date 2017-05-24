//  Copyright (c) 2017 Rackspace
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
	"bytes"
	"flag"
	"fmt"
	"time"

	"github.com/troubling/hummingbird/client"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
)

var Account = ".dispersion"

type DispersionMonitor struct {
	oring  ring.Ring
	logger LowLevelLogger
}

func getDispersionObjects(container string, oring ring.Ring, objNames chan string) {
	defer close(objNames)
	for partition := uint64(0); true; partition++ {
		devs := oring.GetNodesInOrder(partition)
		if devs == nil {
			break
		}
		for i := uint64(0); true; i++ {
			obj := fmt.Sprintf("%d-%d", partition, i)
			genPart := oring.GetPartition(Account, container, obj)
			if genPart == partition {
				objNames <- obj
				break
			}
		}
	}
}

func PutDispersionObjects(hClient client.ProxyClient, container string, policy string) bool {
	status := hClient.PutAccount(Account, common.Map2Headers(map[string]string{
		"Content-Length": "0",
		"Content-Type":   "text",
		"X-Timestamp":    fmt.Sprintf("%d", time.Now().Unix())}))
	if status/100 != 2 {
		fmt.Println(fmt.Sprintf("Could not put account: %v", status))
		return false
	}
	headers := map[string]string{
		"Content-Length": "0",
		"Content-Type":   "text",
		"X-Timestamp":    fmt.Sprintf("%d", time.Now().Unix()),
	}
	if policy != "" {
		headers["X-Storage-Policy"] = policy
	}
	status = hClient.PutContainer(Account, container, common.Map2Headers(headers))
	if status/100 != 2 {
		fmt.Println(fmt.Sprintf("Could not put container: %s %v", container, status))
		return false
	}
	numObjs := uint64(0)
	successes := uint64(0)
	objNames := make(chan string)
	var objRing ring.Ring
	objRing, status = hClient.ObjectRingFor(Account, container)
	if objRing == nil || status != 200 {
		fmt.Println(fmt.Sprintf("Could not obtain object ring: %v", status))
		return false
	}
	go getDispersionObjects(container, objRing, objNames)

	start := time.Now()

	for obj := range objNames {
		numObjs += 1
		if numObjs%1000 == 0 {
			timeSpent := time.Since(start).Seconds()
			partsSec := float64(numObjs) / timeSpent
			hoursRem := float64(objRing.PartitionCount()-numObjs) / partsSec / 60 / 60
			fmt.Println(fmt.Sprintf("So far put %d objects (%.2f/s) %.1fh remaining.", numObjs, partsSec, hoursRem))
		}
		if _, status = hClient.PutObject(Account, container, obj, common.Map2Headers(map[string]string{
			"Content-Length": "0",
			"Content-Type":   "text",
			"X-Timestamp":    fmt.Sprintf("%d", time.Now().Unix())}),
			bytes.NewReader([]byte(""))); status/100 == 2 {
			successes += 1
		} else {
			fmt.Println(fmt.Sprintf("PUT to %s/%s got %v", container, obj, status))
		}
	}
	success := successes == numObjs
	if success {
		fmt.Println(fmt.Sprintf("All %d Dispersion Objects PUT successfully!!", numObjs))
	} else {
		fmt.Println(fmt.Sprintf("Missing %d Dispersion Objects.",
			numObjs-successes))
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

func GetDispersionMonitor(serverconf conf.Config, flags *flag.FlagSet) (Daemon, error) {

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

	logger, err := SetupLogger(serverconf, flags, "andrewd", "dispersion")

	dm := DispersionMonitor{
		oring:  objRing,
		logger: logger,
	}
	return &dm, nil
}
