package dago

import (
	"github.com/ef-ds/deque"
)

type dag map[string][]string // node - and edges from that node

// building the graph
func (d dag) addNode(name string) {
	deps := make([]string, 0)
	d[name] = deps
}

func (d dag) setDownstream(ind, dep string) {
	d[ind] = append(d[ind], dep)
}


// isDownstream checks if a node is dependend or not
func (d dag) isDownstream(nodeName string) bool {
	ind := d.independentNodes()

	for _, name := range ind {
		if nodeName == name {
			return false
		}
	}

	return true
}

// Ensure the DAG is acyclic
func (d dag) validate() bool {
	degree := make(map[string]int)

	for node := range d {
		degree[node] = 0
	}

	for _, ds := range d {
		for _, i := range ds {
			degree[i]++
		}
	}

	var deq deque.Deque

	for node, val := range degree {
		if val == 0 {
			deq.PushFront(node)
		}
	}

	l := make([]string, 0)

	for {
		popped, ok := deq.PopBack()

		if !ok {
			break
		} else {
			node := popped.(string)
			l = append(l, node)
			dsNodes := d[node]
			for _, dsNode := range dsNodes {
				degree[dsNode]--
				if degree[dsNode] == 0 {
					deq.PushFront(dsNode)
				}
			}
		}
	}

	return len(l) == len(d)
} 

// Dependencies return the immediately upstream nodes for a given node
func (d dag) dependencies(curr string) []string {

	dependencies := make([]string, 0)

	for node, edges := range d {
		for _, adj := range edges {
			if curr == adj {
				dependencies = append(dependencies, node)
			}
		}
	}

	return dependencies
}

// independentNodes returns all the independent nodes in the graph
func (d dag) independentNodes() []string {

	downstream := make([]string, 0)

	for _, edges := range d {
		downstream = append(downstream, edges...)
	}

	ind := make([]string, 0)

	for node := range d {
		count := 0
		for _, i := range downstream {
			if node == i {
				count++
			}
		}
		if count == 0 {
			ind = append(ind, node)
		}
	}

	return ind

}
