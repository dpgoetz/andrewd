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
	"fmt"

	"github.com/dpgoetz/andrewd"
)

//	"github.com/openstack/swift/go/hummingbird"

var Version = "0.1"

func main() {
	fmt.Println("hello")

	bc, _ := andrewd.GetBirdCatcher()
	//	fmt.Println("the ring: ", bc.AllDevs())
	reconData, reconErrs := bc.GatherReconData()
	fmt.Println("12345: ", reconData[0].Device, reconData[0].Mounted)
	fmt.Println("12345: ", reconErrs)

	fmt.Println("is it 4: ", len(reconData))

	for _, val := range reconData {
		fmt.Println("reconDev: ", val.Device)
	}
}
