// Package topology validates complete, distinct node coverage before writes.
package topology

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strconv"

	"github.com/mdesousa-fr/qdrant-maintenance/internal/config"
	"github.com/mdesousa-fr/qdrant-maintenance/internal/qdrant"
)

type API interface {
	GetCluster(context.Context) (qdrant.Cluster, error)
	GetCollections(context.Context) ([]qdrant.Collection, error)
}
type Endpoint struct {
	ID     string
	Client API
}
type Node struct {
	ID     string `json:"id"`
	PeerID uint64 `json:"peer_id"`
}
type Pair struct {
	Collection string `json:"collection"`
	Node       string `json:"node"`
}
type Inventory struct {
	ClusterID   string   `json:"cluster_id"`
	Nodes       []Node   `json:"nodes"`
	Collections []string `json:"collections"`
	Pairs       []Pair   `json:"pairs"`
}

func hash(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func (i Inventory) Signature() string { return hash(i) }

// Inspect compares every endpoint's membership and collection inventory. The
// selector is empty for a whole-cluster backup, or names one collection.
func Inspect(ctx context.Context, endpoints []Endpoint, selector string) (Inventory, error) {
	inventory := Inventory{}
	if len(endpoints) == 0 {
		return inventory, fmt.Errorf("no node endpoints configured")
	}
	endpoints = slices.Clone(endpoints)
	sort.Slice(endpoints, func(i, j int) bool { return endpoints[i].ID < endpoints[j].ID })
	var membership []string
	var collections []string
	seenIDs := map[string]bool{}
	seenPeers := map[string]bool{}
	for index, endpoint := range endpoints {
		if !config.ValidSegment(endpoint.ID) || seenIDs[endpoint.ID] {
			return inventory, fmt.Errorf("invalid or duplicate node ID %q", endpoint.ID)
		}
		seenIDs[endpoint.ID] = true
		status, err := endpoint.Client.GetCluster(ctx)
		if err != nil {
			return inventory, fmt.Errorf("node %s cluster status: %w", endpoint.ID, err)
		}
		if status.Status != "enabled" {
			return inventory, fmt.Errorf("node %s is not in distributed mode", endpoint.ID)
		}
		peer := strconv.FormatUint(status.PeerID, 10)
		if _, ok := status.Peers[peer]; !ok {
			return inventory, fmt.Errorf("node %s peer is absent from membership", endpoint.ID)
		}
		if seenPeers[peer] {
			return inventory, fmt.Errorf("duplicate peer %s from node %s; use direct node endpoints", peer, endpoint.ID)
		}
		seenPeers[peer] = true
		peers := []string{}
		for id := range status.Peers {
			if _, err := strconv.ParseUint(id, 10, 64); err != nil {
				return inventory, fmt.Errorf("invalid peer ID %q", id)
			}
			peers = append(peers, id)
		}
		sort.Strings(peers)
		if index == 0 {
			membership = peers
		} else if !slices.Equal(membership, peers) {
			return inventory, fmt.Errorf("inconsistent cluster membership at node %s", endpoint.ID)
		}
		result, err := endpoint.Client.GetCollections(ctx)
		if err != nil {
			return inventory, fmt.Errorf("node %s collections: %w", endpoint.ID, err)
		}
		names := []string{}
		for _, c := range result {
			if !config.ValidSegment(c.Name) {
				return inventory, fmt.Errorf("invalid collection %q", c.Name)
			}
			names = append(names, c.Name)
		}
		sort.Strings(names)
		if len(slices.Compact(slices.Clone(names))) != len(names) {
			return inventory, fmt.Errorf("duplicate collections at node %s", endpoint.ID)
		}
		if index == 0 {
			collections = names
		} else if !slices.Equal(collections, names) {
			return inventory, fmt.Errorf("inconsistent collection inventory at node %s", endpoint.ID)
		}
		inventory.Nodes = append(inventory.Nodes, Node{ID: endpoint.ID, PeerID: status.PeerID})
	}
	if len(seenPeers) != len(membership) {
		return inventory, fmt.Errorf("configured endpoints cover %d of %d peers", len(seenPeers), len(membership))
	}
	inventory.ClusterID = hash(membership)
	inventory.Collections = collections
	selected := collections
	if selector != "" {
		if !slices.Contains(collections, selector) {
			return inventory, fmt.Errorf("collection %q not found", selector)
		}
		selected = []string{selector}
	}
	for _, node := range inventory.Nodes {
		for _, collection := range selected {
			inventory.Pairs = append(inventory.Pairs, Pair{Collection: collection, Node: node.ID})
		}
	}
	if len(inventory.Pairs) == 0 {
		return inventory, fmt.Errorf("no collections to back up")
	}
	return inventory, nil
}
