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
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
)

func getDm(settings ...string) (*DispersionMonitor, string) {
	workDir, err := ioutil.TempDir("", "")
	if err != nil {
		return nil, ""
	}
	confStr, err := conf.StringConfig("")
	if err != nil {
		return nil, fmt.Sprintf("%s", err)
	}
	dm, _ := GetDispersionMonitor(confStr, &flag.FlagSet{})
	return dm.(*DispersionMonitor), workDir
}

func TestGetDispersionObjects(t *testing.T) {

	dm, _ := getDm()
	dObjs := make(chan string)
	go dm.getDispersionObjects(dObjs)
	for val := range dObjs {
		part := dm.oring.GetPartition(Account, Container, val)
		require.Equal(t, strings.Index(val, fmt.Sprintf("%d-", part)), 0)
	}
}

func TestPutDispersionObjects(t *testing.T) {

	dm, _ := getDm()

	var objPaths []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {

		objPaths = append(objPaths, r.URL.Path)
		fmt.Println(fmt.Sprintf("the path: %s", r.URL.Path))
		if _, err := ioutil.ReadAll(r.Body); err != nil {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(201)
	}))
	defer ts.Close()
	u, _ := url.Parse(ts.URL)
	host, ports, _ := net.SplitHostPort(u.Host)
	port, _ := strconv.Atoi(ports)

	fakeDevs := []ring.Device{
		ring.Device{Ip: host, Port: port, Device: "sda"},
		ring.Device{Ip: host, Port: port, Device: "sdb"}}

	dm.oring = &FakeRing{Devs: fakeDevs}

	require.True(t, dm.putDispersionObjects([]string{"0", "1", "2"}))
	require.Equal(t, len(objPaths), 6)
	require.Equal(t, objPaths[0], "/sda/0/.dispersion/dObjects/0")
	require.Equal(t, objPaths[2], "/sda/1/.dispersion/dObjects/1")
	require.Equal(t, objPaths[4], "/sda/2/.dispersion/dObjects/2")

}
