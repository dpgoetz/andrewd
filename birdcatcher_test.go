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
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/openstack/swift/go/hummingbird"
	"github.com/stretchr/testify/require"
)

func getBc(settings ...string) (*BirdCatcher, error) {
	workDir, err := ioutil.TempDir("", "")
	if err != nil {
		return nil, err
	}
	configString := fmt.Sprintf(
		"[andrewd]\nworking_dir=%s\nreport_dir=%s\n", workDir, workDir)
	for i := 0; i < len(settings); i += 2 {
		configString += fmt.Sprintf("%s=%s\n", settings[i], settings[i+1])
	}
	conf, err := hummingbird.StringConfig(configString)
	if err != nil {
		return nil, err
	}

	bc, _ := GetBirdCatcher(conf, &flag.FlagSet{})
	return bc.(*BirdCatcher), nil

}

func closeBc(bc *BirdCatcher) {
	os.RemoveAll(bc.workingDir)
}

func getFakeServer(data string) *httptest.Server {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {

		if _, err := ioutil.ReadAll(r.Body); err != nil {
			w.WriteHeader(500)
			return // nil
		}
		w.WriteHeader(200)
		io.WriteString(w, data)

	}))
	return ts
}

func TestGetReconUmounted(t *testing.T) {
	t.Parallel()
	handlerRan := false

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {

		require.Equal(t, "/recon/unmounted", r.URL.Path)
		handlerRan = true
		if _, err := ioutil.ReadAll(r.Body); err != nil {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
		io.WriteString(w, "[{\"device\": \"sdb1\", \"mounted\": false}]")

	}))
	defer ts.Close()

	u, _ := url.Parse(ts.URL)
	host, ports, _ := net.SplitHostPort(u.Host)
	port, _ := strconv.Atoi(ports)
	dataChan := make(chan *ReconData)
	doneServersChan := make(chan ipPort)

	bc, _ := getBc()
	defer closeBc(bc)

	go bc.reconGetUnmounted(host, port, dataChan, doneServersChan)
	rd := <-dataChan
	require.Equal(t, rd.Device, "sdb1")
	ipp := <-doneServersChan
	require.Equal(t, ipp.ip, host)
	require.Equal(t, ipp.up, true)
	require.Equal(t, handlerRan, true)
}

type FakeRing struct {
	Devs []hummingbird.Device
}

func (r *FakeRing) AllDevices() (devs []hummingbird.Device) {
	return r.Devs
}

func (r *FakeRing) GetNodes(partition uint64) (response []*hummingbird.Device) {
	return nil
}

func (r *FakeRing) GetNodesInOrder(partition uint64) (response []*hummingbird.Device) {
	return nil
}

func (r *FakeRing) GetJobNodes(partition uint64, localDevice int) (response []*hummingbird.Device, handoff bool) {
	return nil, false
}

func (r *FakeRing) GetPartition(account string, container string, object string) uint64 {
	return 0
}

func (r *FakeRing) LocalDevices(localPort int) (devs []*hummingbird.Device, err error) {
	return nil, nil
}

func (r *FakeRing) GetMoreNodes(partition uint64) hummingbird.MoreNodes {
	return nil
}

func TestGatherReconData(t *testing.T) {
	t.Parallel()
	ts := getFakeServer("[{\"device\": \"sdb1\", \"mounted\": true}]")
	defer ts.Close()

	u, _ := url.Parse(ts.URL)
	host, ports, _ := net.SplitHostPort(u.Host)
	port, _ := strconv.Atoi(ports)

	fr := FakeRing{}

	fr.Devs = append(fr.Devs,
		hummingbird.Device{Id: 1, Device: "sdb1", Ip: host, Port: port, Weight: 1})

	bc, _ := getBc()
	defer closeBc(bc)
	bc.oring = &fr

	rds, downServers := bc.gatherReconData([]ipPort{ipPort{ip: host, port: port}})
	require.Equal(t, len(rds), 1)
	require.Equal(t, len(downServers), 0)
	require.Equal(t, rds[0].Mounted, true)
	require.Equal(t, rds[0].Device, "sdb1")
}

func TestGetRingData(t *testing.T) {
	t.Parallel()
	fr := FakeRing{}

	fr.Devs = append(fr.Devs,
		hummingbird.Device{Id: 1, Device: "sdb1", Ip: "1.2", Port: 1, Weight: 1})
	fr.Devs = append(fr.Devs,
		hummingbird.Device{Id: 2, Device: "sdb2", Ip: "1.2", Port: 1, Weight: 1})
	fr.Devs = append(fr.Devs,
		hummingbird.Device{Id: 2, Device: "sdb1", Ip: "1.2", Port: 2, Weight: 1})
	fr.Devs = append(fr.Devs,
		hummingbird.Device{Id: 2, Device: "sdb1", Ip: "1.2", Port: 3, Weight: 0})

	bc, _ := getBc()
	defer closeBc(bc)
	bc.oring = &fr

	allRingDevices, allWeightedServers := bc.getRingData()

	require.Equal(t, len(allRingDevices), 4)
	require.Equal(t, len(allWeightedServers), 2)

	devMap := make(map[string]bool)
	for _, dev := range allRingDevices {
		devMap[bc.deviceId(dev.Ip, dev.Port, dev.Device)] = true
	}
	require.Equal(t, len(devMap), 4)
}

func TestPopulateDbWithRing(t *testing.T) {
	t.Parallel()
	ts := getFakeServer("[{\"device\": \"sdb1\", \"mounted\": true}]")
	defer ts.Close()
	fr := FakeRing{}

	u, _ := url.Parse(ts.URL)
	host, ports, _ := net.SplitHostPort(u.Host)
	port, _ := strconv.Atoi(ports)

	fr.Devs = append(fr.Devs,
		hummingbird.Device{Id: 1, Device: "sdb1", Ip: "1.2", Port: 1, Weight: 1.1})
	fr.Devs = append(fr.Devs,
		hummingbird.Device{Id: 2, Device: "sdb2", Ip: "1.2", Port: 1, Weight: 3.5})
	fr.Devs = append(fr.Devs,
		hummingbird.Device{Id: 2, Device: "sdb3", Ip: host, Port: port, Weight: 5.5})

	bc, _ := getBc()
	defer closeBc(bc)
	bc.oring = &fr

	bc.updateDb()

	db, err := bc.getDb()

	require.Equal(t, err, nil)

	rows, _ := db.Query("SELECT ip, port, device, weight, mounted, reachable FROM device")
	devMap := make(map[string]float64)
	cnt := 0
	for rows.Next() {
		var ip, device string
		var port int
		var mounted, reachable bool
		var weight float64

		err := rows.Scan(&ip, &port, &device, &weight, &mounted, &reachable)
		require.Equal(t, err, nil)
		devMap[bc.deviceId(ip, int(port), device)] = weight
		require.Equal(t, mounted, true)
		if device == "sdb3" {
			require.Equal(t, reachable, true)
		} else {
			require.Equal(t, reachable, false)
		}
		cnt += 1

	}
	require.Equal(t, cnt, 3)
	require.Equal(t, devMap["1.2:1/sdb1"], 1.1)
	require.Equal(t, devMap["1.2:1/sdb2"], 3.5)
	require.Equal(t, devMap[fmt.Sprintf("%s:%d/sdb3", host, port)], 5.5)
}

func TestUpdateDb(t *testing.T) {
	t.Parallel()
	ts := getFakeServer("[{\"device\": \"sdb1\", \"mounted\": false}]")
	defer ts.Close()
	fr := FakeRing{}

	u, _ := url.Parse(ts.URL)
	host, ports, _ := net.SplitHostPort(u.Host)
	port, _ := strconv.Atoi(ports)

	fr.Devs = append(fr.Devs,
		hummingbird.Device{Id: 1, Device: "sdb1", Ip: host, Port: port, Weight: 1.1})

	bc, _ := getBc()
	defer closeBc(bc)
	bc.oring = &fr

	db, _ := bc.getDb()
	defer db.Close()

	tx, err := db.Begin()
	require.Equal(t, err, nil)

	_, err = tx.Exec("INSERT INTO device "+
		"(ip, port, device, in_ring, weight, mounted, reachable) VALUES"+
		"(?,?,?,?,?,?,?)", host, port, "sdb1", true, 2.3, true, true)
	require.Equal(t, err, nil)
	require.Equal(t, tx.Commit(), nil)

	bc.updateDb()

	rows, _ := db.Query("SELECT ip, port, device, weight, mounted, reachable FROM device")
	cnt := 0
	for rows.Next() {
		var ip, device string
		var port int
		var mounted, reachable bool
		var weight float64

		err := rows.Scan(&ip, &port, &device, &weight, &mounted, &reachable)
		require.Equal(t, err, nil)
		require.Equal(t, mounted, false)
		require.Equal(t, reachable, true)
		require.Equal(t, weight, 1.1)
		cnt += 1
	}
	rows.Close()
	require.Equal(t, cnt, 1)
}

func TestUpdateRing(t *testing.T) {
	t.Parallel()

	bc, _ := getBc()

	db, _ := bc.getDb()
	defer closeBc(bc)
	defer db.Close()

	tx, err := db.Begin()
	require.Equal(t, err, nil)

	now := time.Now()
	_, err = tx.Exec("INSERT INTO device "+
		"(ip, port, device, in_ring, weight, mounted, reachable, last_update) VALUES"+
		"(?,?,?,?,?,?,?,?)", "1.2.3.4", 6000, "sdb1", true, 2.3, false, true, now.AddDate(0, 0, -8))
	_, err = tx.Exec("INSERT INTO Device "+
		"(ip, port, device, in_ring, weight, mounted, reachable, last_update) VALUES"+
		"(?,?,?,?,?,?,?,?)", "1.2.3.4", 6000, "sdb2", true, 2.3, false, true, now.AddDate(0, 0, -3))
	require.Equal(t, err, nil)
	require.Equal(t, tx.Commit(), nil)

	ringBuilder := fmt.Sprintf("%s/object.builder", bc.workingDir)
	bc.ringBuilder = ringBuilder
	cmd := exec.Command("swift-ring-builder", ringBuilder, "create", "4", "1", "1")

	require.Equal(t, cmd.Run(), nil)

	cmd = exec.Command("swift-ring-builder", ringBuilder, "add", "r1z1-1.2.3.4:6000/sdb1", "1")
	require.Equal(t, cmd.Run(), nil)

	cmd = exec.Command("swift-ring-builder", ringBuilder, "add", "r1z1-1.2.3.4:6000/sdb2", "1")
	require.Equal(t, cmd.Run(), nil)

	_, err = os.Stat(ringBuilder)
	require.Equal(t, err, nil)

	_, err = bc.updateRing()
	require.Equal(t, err, nil)

	cmd = exec.Command("swift-ring-builder", ringBuilder, "search", "--device=sdb1", "--weight=1")
	var out bytes.Buffer
	cmd.Stdout = &out
	require.Equal(t, cmd.Run(), nil)

	cmd = exec.Command("swift-ring-builder", ringBuilder,
		"pretend_min_part_hours_passed")
	require.Equal(t, cmd.Run(), nil)
	bc.maxWeightChange = .9
	outTxt, err := bc.updateRing()
	require.Equal(t, err, nil)
	require.True(t, strings.Index(outTxt, "Reassigned 8 (50.00%) partitions.") > 0)

	cmd = exec.Command("swift-ring-builder", ringBuilder, "search", "--device=sdb1", "--weight=1")
	cmd.Stdout = &out
	require.NotEqual(t, cmd.Run(), nil)

	cmd = exec.Command("swift-ring-builder", ringBuilder, "search", "--device=sdb1", "--weight=0")
	require.Equal(t, cmd.Run(), nil)

	cmd = exec.Command("swift-ring-builder", ringBuilder, "search", "--device=sdb2", "--weight=1")
	require.Equal(t, cmd.Run(), nil)

	rows, err := db.Query("SELECT count(*) FROM ring_action WHERE device=? and action=?", "sdb1", "ZEROED")
	var cnt int
	if rows != nil {
		rows.Next()
		rows.Scan(&cnt)
		require.Equal(t, cnt, 1)
	}

	rows, err = db.Query("SELECT count(*) FROM ring_action WHERE device=? and action=?", "sdb2", "ZEROED")
	if rows != nil {
		rows.Next()
		rows.Scan(&cnt)
		require.Equal(t, cnt, 0)
	}

	rData, err := bc.getReportData()
	require.Equal(t, err, nil)
	require.Equal(t, rData.TotalDevices, 2)
	require.Equal(t, len(rData.LastRingZeroes), 1)
	require.Equal(t, rData.LastRingZeroes[0].Device, "sdb1")
}

func TestProduceReport(t *testing.T) {
	t.Parallel()

	bc, _ := getBc()
	defer closeBc(bc)
	db, _ := bc.getDb()
	defer db.Close()
	tx, err := db.Begin()
	require.Equal(t, err, nil)

	_, err = tx.Exec("INSERT INTO device "+
		"(ip, port, device, in_ring, weight, mounted, reachable) VALUES"+
		"(?,?,?,?,?,?,?)", "1.2.3.4", 6000, "sdb1", true, 2.3, true, true)
	require.Equal(t, err, nil)
	require.Equal(t, tx.Commit(), nil)

	rData, err := bc.getReportData()
	require.Equal(t, err, nil)
	require.Equal(t, rData.TotalDevices, 1)
	require.Equal(t, rData.TotalWeight, 2.3)
	require.Equal(t, len(rData.UnmountedDevices), 0)
	require.Equal(t, len(rData.LastRingZeroes), 0)

	tx, err = db.Begin()
	require.Equal(t, err, nil)

	_, err = tx.Exec("INSERT INTO device "+
		"(ip, port, device, in_ring, weight, mounted, reachable) VALUES"+
		"(?,?,?,?,?,?,?)", "1.2.3.4", 6000, "sdb2", true, 0, false, true)
	require.Equal(t, err, nil)
	require.Equal(t, tx.Commit(), nil)

	rData, err = bc.getReportData()
	require.Equal(t, err, nil)
	require.Equal(t, rData.TotalDevices, 2)
	require.Equal(t, rData.TotalWeight, 2.3)
	require.Equal(t, len(rData.UnmountedDevices), 1)
	require.Equal(t, len(rData.LastRingZeroes), 0)
}

func TestNeedRingUpdate(t *testing.T) {
	t.Parallel()

	bc, _ := getBc()
	defer closeBc(bc)
	db, _ := bc.getDb()
	defer db.Close()
	tx, err := db.Begin()
	require.Equal(t, err, nil)

	require.False(t, bc.needRingUpdate())

	now := time.Now()
	_, err = tx.Exec("INSERT INTO ring_action "+
		"(ip, port, device, action, create_date) VALUES"+
		"(?,?,?,?,?)", "1.2.3.4", 6000, "sdb1", "ZEROED", now)
	_, err = tx.Exec("INSERT INTO ring_action "+
		"(ip, port, device, action, create_date) VALUES"+
		"(?,?,?,?,?)", "1.2.3.4", 6000, "sdb1", "ZEROED", now)
	require.Equal(t, err, nil)
	require.Equal(t, tx.Commit(), nil)
	time.Sleep(10000)

	bc.ringUpdateFreq = 10
	require.True(t, bc.needRingUpdate())
	bc.ringUpdateFreq = 1000000000
	require.False(t, bc.needRingUpdate())
}
