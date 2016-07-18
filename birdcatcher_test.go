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

package andrewd

import (
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/openstack/swift/go/hummingbird"
	"github.com/stretchr/testify/require"
)

func TestGetReconUmounted(t *testing.T) {
	t.Parallel()
	handlerRan := false

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {

		require.Equal(t, "/recon/unmounted", r.URL.Path)
		fmt.Println("asdfdsa")
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

	bc, _ := GetBirdCatcher()

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

	//	devs = append(devs,
	//		hummingbird.Device{Id: 1, Device: "sda", Ip: "127.0.0.1", Port: 1})
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
	handlerRan := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {

		require.Equal(t, "/recon/unmounted", r.URL.Path)
		fmt.Println("asdfdsa")
		handlerRan = true
		if _, err := ioutil.ReadAll(r.Body); err != nil {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
		io.WriteString(w, "[{\"device\": \"sdb1\", \"mounted\": true}]")

	}))
	defer ts.Close()

	u, _ := url.Parse(ts.URL)
	host, ports, _ := net.SplitHostPort(u.Host)
	port, _ := strconv.Atoi(ports)

	fr := FakeRing{}

	fr.Devs = append(fr.Devs,
		hummingbird.Device{Id: 1, Device: "sdb1", Ip: host, Port: port, Weight: 1})

	bc, _ := GetBirdCatcher()
	bc.oring = &fr

	rds, downServers := bc.gatherReconData([]ipPort{ipPort{ip: host, port: port}})
	fmt.Println("asdfadsfsadf: ", rds[0])
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

	bc, _ := GetBirdCatcher()
	bc.oring = &fr

	allRingDevices, allWeightedServers := bc.getRingData()

	require.Equal(t, len(allRingDevices), 4)
	require.Equal(t, len(allWeightedServers), 2)

}
