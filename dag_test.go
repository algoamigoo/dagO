package dago

import (
	"testing"
)

func TestDag(t *testing.T) {
	d := make(dag)

	d.addNode("a")
	d.addNode("b")
	d.addNode("c")
	d.addNode("d")
	d.setDownstream("a", "b")
	d.setDownstream("a", "c")
	d.setDownstream("b", "d")
	d.setDownstream("c", "d")

	if !d.validate() {
		t.Errorf("Valid dag failed validation check")
	}

	if !equal(d.dependencies("b"), []string{"a"}) {
		t.Errorf("d.dependencies() returned %s, expected %s",
			d.dependencies("b"),
			[]string{"a"})
	}

	if !equal(d.independentNodes(), []string{"a"}) {
		t.Errorf("d.independentNodes() returned %s, expected %s",
			d.dependencies("b"),
			[]string{"a"})
	}

	e := make(dag)

	e.addNode("a")
	e.addNode("b")
	e.addNode("c")
	e.setDownstream("c", "a")
	e.setDownstream("a", "b")
	e.setDownstream("b", "a")

	if e.validate() {
		t.Errorf("Invalid dag passed validation check")
	}
}

func TestDagWithSingleNode(t *testing.T) {
	d := make(dag)
	d.addNode("a")
	res := d.isDownstream("a")

	if res {
		t.Errorf("isDownstream() returned true for an independent node")
	}
}

func TestDagMultiIndependentNodes(t *testing.T) {
    d := make(dag)
    d.addNode("a")
    d.addNode("b")
    d.setDownstream("a", "c")
    d.setDownstream("b", "d")

    ind := d.independentNodes()
    // Expected: both "a" and "b" should be independent
    if len(ind) != 2 {
        t.Errorf("Expected 2 independent nodes, got %d", len(ind))
    }
}

func TestDagEmpty(t *testing.T) {
    d := make(dag)

    if !d.validate() {
        t.Errorf("Empty DAG should be considered valid")
    }

    if len(d.independentNodes()) != 0 {
        t.Errorf("Empty DAG should have 0 independent nodes")
    }
}
