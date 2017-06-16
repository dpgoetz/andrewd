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
	"io"
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
	container := "objs"
	go getDispersionObjects(container, oring, dObjs)
	for val := range dObjs {
		part := oring.GetPartition(Account, container, val)
		require.Equal(t, strings.Index(val, fmt.Sprintf("%d-", part)), 0)
	}
}

func TestPutDispersionObjects(t *testing.T) {
	c := &testPutDispersionObjectsClient{objRing: &FakeRing{Devs: []ring.Device{{Device: "sda"}, {Device: "sdb"}, {Device: "sdc"}}}}
	require.True(t, PutDispersionObjects(c, "objs", ""))
	require.Equal(t, 4, c.objPuts)
}

type testPutDispersionObjectsClient struct {
	objRing ring.Ring
	objPuts int
}

func (c *testPutDispersionObjectsClient) PutAccount(account string, headers http.Header) *http.Response {
	return client.ResponseStub(200, "")
}

func (c *testPutDispersionObjectsClient) PostAccount(account string, headers http.Header) *http.Response {
	return client.ResponseStub(200, "")
}

func (c *testPutDispersionObjectsClient) GetAccount(account string, options map[string]string, headers http.Header) *http.Response {
	return client.ResponseStub(200, "")
}

func (c *testPutDispersionObjectsClient) HeadAccount(account string, headers http.Header) *http.Response {
	return client.ResponseStub(200, "")
}

func (c *testPutDispersionObjectsClient) DeleteAccount(account string, headers http.Header) *http.Response {
	return client.ResponseStub(200, "")
}

func (c *testPutDispersionObjectsClient) PutContainer(account string, container string, headers http.Header) *http.Response {
	return client.ResponseStub(200, "")
}

func (c *testPutDispersionObjectsClient) PostContainer(account string, container string, headers http.Header) *http.Response {
	return client.ResponseStub(200, "")
}

func (c *testPutDispersionObjectsClient) GetContainer(account string, container string, options map[string]string, headers http.Header) *http.Response {
	return client.ResponseStub(200, "")
}

func (c *testPutDispersionObjectsClient) GetContainerInfo(account string, container string) *client.ContainerInfo {
	return nil
}

func (c *testPutDispersionObjectsClient) HeadContainer(account string, container string, headers http.Header) *http.Response {
	return client.ResponseStub(200, "")
}

func (c *testPutDispersionObjectsClient) DeleteContainer(account string, container string, headers http.Header) *http.Response {
	return client.ResponseStub(200, "")
}

func (c *testPutDispersionObjectsClient) PutObject(account string, container string, obj string, headers http.Header, src io.Reader) *http.Response {
	c.objPuts++
	return client.ResponseStub(200, "")
}

func (c *testPutDispersionObjectsClient) PostObject(account string, container string, obj string, headers http.Header) *http.Response {
	return client.ResponseStub(200, "")
}

func (c *testPutDispersionObjectsClient) GetObject(account string, container string, obj string, headers http.Header) *http.Response {
	return client.ResponseStub(200, "")
}

func (c *testPutDispersionObjectsClient) HeadObject(account string, container string, obj string, headers http.Header) *http.Response {
	return client.ResponseStub(200, "")
}

func (c *testPutDispersionObjectsClient) DeleteObject(account string, container string, obj string, headers http.Header) *http.Response {
	return client.ResponseStub(200, "")
}

func (c *testPutDispersionObjectsClient) ObjectRingFor(account string, container string) (ring.Ring, *http.Response) {
	return c.objRing, client.ResponseStub(200, "")
}
