package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// “… term 12, nodeID node3, state Leader …”
var lineRE = regexp.MustCompile(
	`\bterm\s+(\d+)\s*,\s*nodeID\s+(\S+?)\s*,\s*state\s+([A-Za-z]+)\b`,
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <logfile> [<logfile> ...]\n", os.Args[0])
		os.Exit(1)
	}

	leadersByTerm := map[int]map[string]struct{}{}  // term → set[nodeID]
	termsSeen := map[int]bool{}                     // term mentioned at all
	nodeStatesByTerm := map[int]map[string]string{} // term → nodeID → state
	minTerm, maxTerm := math.MaxInt, math.MinInt    // track range

	for _, path := range os.Args[1:] {
		if err := processFile(path, leadersByTerm, termsSeen, nodeStatesByTerm, &minTerm, &maxTerm); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
		}
	}

	report(minTerm, maxTerm, termsSeen, leadersByTerm, nodeStatesByTerm)
}

func processFile(
	path string,
	leadersByTerm map[int]map[string]struct{},
	termsSeen map[int]bool,
	nodeStatesByTerm map[int]map[string]string,
	minTerm, maxTerm *int,
) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.Trim(sc.Text(), "#") // remove leading/trailing '#'
		m := lineRE.FindStringSubmatch(line)
		if len(m) != 4 {
			continue // not a log line
		}

		termNum, _ := strconv.Atoi(m[1])
		nodeID, state := m[2], m[3]

		termsSeen[termNum] = true
		if termNum < *minTerm {
			*minTerm = termNum
		}
		if termNum > *maxTerm {
			*maxTerm = termNum
		}

		// Track node state for this term
		if nodeStatesByTerm[termNum] == nil {
			nodeStatesByTerm[termNum] = make(map[string]string)
		}
		nodeStatesByTerm[termNum][nodeID] = state

		if state == "Leader" {
			set := leadersByTerm[termNum]
			if set == nil {
				set = make(map[string]struct{})
				leadersByTerm[termNum] = set
			}
			set[nodeID] = struct{}{}
		}
	}
	return sc.Err()
}

func report(
	minTerm, maxTerm int,
	termsSeen map[int]bool,
	leadersByTerm map[int]map[string]struct{},
	nodeStatesByTerm map[int]map[string]string,
) {
	if len(termsSeen) == 0 {
		fmt.Println("no terms found in the supplied files")
		return
	}

	for term := minTerm; term <= maxTerm; term++ {
		if !termsSeen[term] {
			fmt.Printf("term %-6d  MISSING\n", term)
			continue
		}

		nodes := leadersByTerm[term]
		nodeStates := nodeStatesByTerm[term]
		nodeList := sortedNodeIDs(nodeStates)
		stateList := make([]string, 0, len(nodeList))
		for _, node := range nodeList {
			stateList = append(stateList, nodeStates[node])
		}
		stateCol := fmt.Sprintf("[%s]", strings.Join(stateList, ", "))

		switch len(nodes) {
		case 0:
			fmt.Printf("term %-6d  NO LEADER   %s\n", term, stateCol)
		case 1:
			fmt.Printf("term %-6d  OK          %s\n", term, stateCol)
		default:
			fmt.Printf("term %-6d  FAIL        %s\n", term, stateCol)
		}
	}
}

func keys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

func sortedNodeIDs(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
