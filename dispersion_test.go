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
	"github.com/troubling/hummingbird/client"
	"github.com/troubling/hummingbird/common/ring"
)

func TestGetDispersionObjects(t *testing.T) {

	var reqPaths []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {

		reqPaths = append(reqPaths, r.URL.Path)
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
		{Ip: host, Port: port, Device: "sda"},
		{Ip: host, Port: port, Device: "sdb"}}

	oring := &FakeRing{Devs: fakeDevs}
	dObjs := make(chan string)
	go getDispersionObjects(oring, dObjs)
	for val := range dObjs {
		part := oring.GetPartition(Account, Container, val)
		require.Equal(t, strings.Index(val, fmt.Sprintf("%d-", part)), 0)
	}
}

func TestPutDispersionObjects(t *testing.T) {

	var reqPaths []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {

		reqPaths = append(reqPaths, r.URL.Path)
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
		{Ip: host, Port: port, Device: "sda"},
		{Ip: host, Port: port, Device: "sdb"},
		{Ip: host, Port: port, Device: "sdc"}}

	aring := &FakeRing{Devs: fakeDevs}
	cring := &FakeRing{Devs: fakeDevs}
	oring := &FakeRing{Devs: fakeDevs}
	hClient, err := client.NewProxyDirectClientWithRings(
		aring, cring, oring)
	require.Equal(t, err, nil)
	//hClient.OverrideRings(aring, cring, oring)

	require.True(t, PutDispersionObjects(hClient, oring))
	require.Equal(t, len(reqPaths), 18)

}
