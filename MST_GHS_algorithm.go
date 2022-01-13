/*
	Project : MINIMUM SPANNING TREE (MST) CONSTRUCTION WITH GALLAGER-HUMBLET-SPIRA (GHS) ALGORITHM
	Author : ANTOINE LESTRADE, GUILLAUME DIGIER
	Date : JANUARY 2021
*/

package main

//---------------------
// IMPORTS
//---------------------

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
)

//---------------------
// PREPERATIONS
//---------------------

var PORT string = ":30000"

var MST = make(map[string][]string)

type yamlConfig struct {
	ID      int    `yaml:"id"`
	Address string `yaml:"address"`

	Neighbours []struct {
		ID         int    `yaml:"id"`
		Address    string `yaml:"address"`
		EdgeWeight int    `yaml:"edge_weight"`
	} `yaml:"neighbours"`
}

//-------------------------
// PREPARATING FUNCTIONS
//-------------------------

func initAndParseFileNeighbours(filename string) yamlConfig {

	fullpath, _ := filepath.Abs("./Neighbours/" + filename)
	yamlFile, err := ioutil.ReadFile(fullpath)

	if err != nil {
		panic(err)
	}

	var data yamlConfig

	err = yaml.Unmarshal([]byte(yamlFile), &data)

	if err != nil {
		panic(err)
	}

	return data
}

//-------------------------
// COMMUNICATING FUNCTIONS
//-------------------------

func send(message string, nodeAddress string, neighAddress string) {

	myLog(nodeAddress, "Sending message \""+strings.Split(message, ",")[0]+"\" to "+neighAddress)

	// Il faut bien preciser l'adresse de l'envoyeur si on veut que le recepteur puisse la connaitre.
	d := net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP(nodeAddress)}}

	outConn, err := d.Dial("tcp", neighAddress+PORT)

	if err != nil {
		log.Fatal(err)

		return
	}

	outConn.Write([]byte(message))
	outConn.Close()
}

//-------------------------
// UTILITY FUNCTIONS
//-------------------------

func myLog(localAdress string, message string) {

	fmt.Printf("[%s] : %s\n", localAdress, message)
}

func getLeastWeightEdge(node yamlConfig) (string, int) { // GUILLAUME DIGIER

	var min int = 0
	var leastNeighbour string

	for i, neighbour := range node.Neighbours {
		if i == 0 || neighbour.EdgeWeight < min {
			min = neighbour.EdgeWeight
			leastNeighbour = neighbour.Address
		}
	}

	return leastNeighbour, min
}

func getWeight(address string, node yamlConfig) int { // ANTOINE LESTRADE

	for _, neighbour := range node.Neighbours {
		if address == neighbour.Address {
			return neighbour.EdgeWeight
		}
	}

	fmt.Println("WEIGHT NOT FOUND IN NEIGHBOURS")
	os.Exit(-1)
	return -1
}

func deleteDuplicatesAndReturnOne(myMap map[int]int) int {

	for key1, value := range myMap {
		for key2, value2 := range myMap {
			if value == value2 && key1 != key2 {
				delete(myMap, key2)
				return key1
			}
		}
	}

	fmt.Println("NOT FOUND SIMILAR VALUES IN MAP")
	os.Exit(-1)
	return -1
}

//-------------------------
// GLOBAL VARIABLES
//-------------------------

// Static

var BASIC string = "Basic"
var BRANCH string = "Branch"
var REJECTED string = "Rejected"

var SLEEPING string = "Sleeping"
var FOUND string = "Found"
var FIND string = "Find"

var INFINITY int = math.MaxInt64

// Non-static

var edgesMST map[int]int = make(map[int]int)

//-------------------------
// THE SERVER FUNCTION
//-------------------------

func server(neighboursFilePath string, isSynchrone bool) map[int]int {

	// --- PREPARE THE CONFIGURATION --- //

	var node yamlConfig = initAndParseFileNeighbours(neighboursFilePath)

	myLog(node.Address, "Neighbours file parsing ...")
	myLog(node.Address, "Done ...")

	time.Sleep(1 * time.Second) // For synchronization only !

	ln, err := net.Listen("tcp", node.Address+PORT)

	if err != nil {
		log.Fatal(err)

		fmt.Println("CONNECTION ERROR")
		os.Exit(-1)
	}

	myLog(node.Address, "Starting server .... and listening ...")

	time.Sleep(1 * time.Second) // For synchronization only !

	// --- PREPARE VARIABLES --- //
	// GUILLAUME DIGIER

	var terminated bool = false

	var statusEdge map[string]string = make(map[string]string)
	for _, neighbour := range node.Neighbours {
		statusEdge[neighbour.Address] = BASIC
	}

	var m string

	var levelNode int
	var state string = SLEEPING
	var findCount int
	var fragmentNode int = -1

	var parent string
	var bestWt int
	var bestNode string
	var testNode string

	// --- RE-USABLE PROCEDURES --- //

	// Procedure "report"
	// ANTOINE LESTRADE (AVEC GUILLAUME DIGIER)
	report := func() {
		if (findCount == 0) && (testNode == "") {
			state = FOUND
			go send("Report,"+strconv.Itoa(bestWt), node.Address, parent)
		}
	}

	// Procedure "test"
	// ANTOINE LESTRADE
	test := func() {
		var min int = 0
		var leastNeighbour string = ""
		var mapNeighbours map[string]int = make(map[string]int)

		for _, neighbour := range node.Neighbours {
			if statusEdge[neighbour.Address] == BASIC {
				mapNeighbours[neighbour.Address] = neighbour.EdgeWeight
			}
		}

		var i int = 0

		for key, value := range mapNeighbours {
			if i == 0 || value < min {
				min = value
				leastNeighbour = key
			}
			i += 1
		}

		if leastNeighbour != "" {
			testNode = leastNeighbour
			go send("Test,"+strconv.Itoa(levelNode)+","+strconv.Itoa(fragmentNode), node.Address, testNode)
		} else {
			testNode = ""

			// Procedure "report"
			report()
		}
	}

	// Procedure "changeRoot"
	// GUILLAUME DIGIER
	changeRoot := func() {
		if statusEdge[bestNode] == BRANCH {
			go send("ChangeRoot,", node.Address, bestNode)
		} else {
			statusEdge[bestNode] = BRANCH
			go send("Connect,"+strconv.Itoa(levelNode), node.Address, bestNode)
		}
	}

	// Procedure "wait"
	// ANTOINE LESTRADE
	wait := func(m string, j string) {

		// This procedure is used when a node "should" have received another message m' first instead of message m.
		// So it waits to give itself the opportunity to receive the message m' and then resend the message m to himself.
		// This way, the message m will be put at the end of the receiving list and will STILL be treated.

		time.Sleep(1 * time.Second)
		go send(m, j, node.Address)
	}

	// --- THE ALGORITHM --- //

	myLog(node.Address, "Starting algorithm ...")

	// (1) WakeUp
	// GUILLAUME DIGIER

	m, _ = getLeastWeightEdge(node)

	statusEdge[m] = BRANCH
	levelNode = 0
	state = FOUND
	findCount = 0

	time.Sleep(1 * time.Second) // For synchronization only !

	go send("Connect,"+strconv.Itoa(levelNode), node.Address, m)

	// The message listening loop

	for !terminated {

		// Reception of messages

		conn, _ := ln.Accept() // Reading/waiting for messages (BLOCKING) !
		message, _ := bufio.NewReader(conn).ReadString('\n')
		conn.Close()

		// Getting the message sender

		j := conn.RemoteAddr().String()
		j = j[:len(j)-6] // Without the port number.

		myLog(node.Address, "Message received \""+message+"\" from "+j)

		// GUILLAUME DIGIER : Comment envoyer / recevoir plusieurs informations en un seul message (utilisation du split).
		message_splitted := strings.Split(message, ",") // Splitting message by ","

		// First string of message is the message type
		switch message_splitted[0] {

		// (2) Connect
		// ANTOINE LESTRADE
		case "Connect":

			L, _ := strconv.Atoi(message_splitted[1])

			if L < levelNode { // When the other fragment asks to connect and its level is smaller
				statusEdge[j] = BRANCH
				go send("Initiate,"+strconv.Itoa(levelNode)+","+strconv.Itoa(fragmentNode)+","+state, node.Address, j)

				if state == FIND {
					findCount += 1
				}
			} else if statusEdge[j] == BASIC {
				// Procedure "wait"
				wait(message, j)
			} else { // When both fragment want to connect to eachother
				go send("Initiate,"+strconv.Itoa(levelNode+1)+","+strconv.Itoa(getWeight(j, node))+","+FIND, node.Address, j)
			}

		// (3) Initiate
		// ANTOINE LESTRADE
		case "Initiate":

			levelNode, _ = strconv.Atoi(message_splitted[1])
			fragmentNode, _ = strconv.Atoi(message_splitted[2])
			state = message_splitted[3] // the state is set

			parent = j

			// Propagate the update
			bestNode = ""
			bestWt = INFINITY

			// Inform the neighbours which are part of the fragment as well
			for _, r := range node.Neighbours {
				if statusEdge[r.Address] == BRANCH && r.Address != j {
					go send("Initiate,"+strconv.Itoa(levelNode)+","+strconv.Itoa(fragmentNode)+","+state, node.Address, r.Address)
					if state == FIND {
						findCount += 1
					}
				}
			}

			// Find the least weight edge
			if state == FIND {

				// Procedure "test"
				test()
			}

		// (4) Test
		// ANTOINE LESTRADE
		case "Test":

			L, _ := strconv.Atoi(message_splitted[1])
			F, _ := strconv.Atoi(message_splitted[2])

			if L > levelNode {
				// Procedure "wait"
				wait(message, j)
			} else if F != fragmentNode {
				go send("Accept", node.Address, j)

			} else {
				if statusEdge[j] == BASIC {
					statusEdge[j] = REJECTED
				}
				if testNode != j {
					go send("Reject", node.Address, j)
				} else {
					// Procedure "test"
					test()
				}
			}

		// (5) Accept
		// ANTOINE LESTRADE
		case "Accept":

			testNode = ""
			var jWeight int = getWeight(j, node)

			if jWeight < bestWt {
				bestNode = j
				bestWt = jWeight
			}

			// Procedure "report"
			report()

		// (6) Reject
		// ANTOINE LESTRADE
		case "Reject":

			if statusEdge[j] == BASIC {
				statusEdge[j] = REJECTED
			}

			// Procedure "test"
			test()

		// (7) Report
		// GUILLAUME DIGIER
		case "Report":

			w, _ := strconv.Atoi(message_splitted[1])

			if j != parent {

				findCount -= 1

				if w < bestWt {
					bestWt = w
					bestNode = j
				}
				// Procedure "report"
				report()
			} else if state == FIND {
				// Procedure "wait"
				wait(message, j)
			} else if w > bestWt {
				// Procedure "changeRoot"
				changeRoot()
			} else if w == bestWt && w == INFINITY {
				myLog(node.Address, "Core-root is stopping ...")
				terminated = true
			}

		// (8) ChangeRoot
		// GUILLAUME DIGIER
		case "ChangeRoot":

			// Procedure "changeRoot"
			changeRoot()

		// (9) Stop
		// GUILLAUME DIGIER
		case "Stop":

			myLog(node.Address, "Stopping because "+j+" asked to stop the execution ...")
			terminated = true
		}
	}

	// When a node is stopping, send his BRANCH neighbours a "Stop" message to stop as well
	// GUILLAUME DIGIER
	for _, neighbour := range node.Neighbours {
		if statusEdge[neighbour.Address] == BRANCH && neighbour.Address != parent {
			go send("Stop", node.Address, neighbour.Address)
		}
	}

	// For displaying the results at the end of the program
	// GUILLAUME DIGIER

	// Add the edge the ascending edge (going to his parent) to the list of edge part of the Minimum Spanning Tree (MST)
	edgesMST[node.ID] = getWeight(parent, node)

	// Return the appropriate value depending if current node is executed synchronously or asynchronously
	if isSynchrone {
		return edgesMST
	} else {
		return nil
	}
}

//---------------------
// MAIN
//---------------------

func main() {

	go server("node-2.yaml", false)
	go server("node-3.yaml", false)
	go server("node-4.yaml", false)
	go server("node-5.yaml", false)
	go server("node-6.yaml", false)
	go server("node-7.yaml", false)
	go server("node-8.yaml", false)
	go server("node-9.yaml", false)

	edgesMST := server("node-1.yaml", true)

	time.Sleep(2 * time.Second) // Waiting all console return from nodes

	fmt.Println("\n=====================================================================================================")
	fmt.Println("The following edges ONLY are part of the Minimum Spanning Tree (as shown on 'node_end_graph.png')")
	fmt.Println("=====================================================================================================")

	coreRootKey := deleteDuplicatesAndReturnOne(edgesMST)

	for key, value := range edgesMST {
		if key == coreRootKey {
			fmt.Println(value, "(Core-root)")
		} else {
			fmt.Println(value)
		}
	}

	fmt.Println("=====================================")
	fmt.Println("End")
	fmt.Println("=====================================")
}
