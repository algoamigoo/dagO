package dago

type NodeID string

type Edge struct {
    From, To NodeID
}
type Graph struct {
    Nodes map[NodeID]bool
    Edges []Edge
}