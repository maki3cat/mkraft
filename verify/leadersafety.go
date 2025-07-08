package main

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
)

// Matches “… term N, nodeID X, state Y …”
var lineRE = regexp.MustCompile(`\bterm (\d+),\s+nodeID (\S+),\s+state (\S+)`)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <logfile> [<logfile> ...]\n", os.Args[0])
		os.Exit(1)
	}

	// term → set[nodeID] for lines where state == Leader
	leadersByTerm := map[int]map[string]struct{}{}

	// term → bool (seen at least once)
	termsSeen := map[int]bool{}

	for _, path := range os.Args[1:] {
		if err := processFile(path, leadersByTerm, termsSeen); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
		}
	}

	report(termsSeen, leadersByTerm)
}

func processFile(path string, leadersByTerm map[int]map[string]struct{}, termsSeen map[int]bool) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		m := lineRE.FindStringSubmatch(sc.Text())
		if len(m) != 4 {
			continue
		}
		termNum, _ := strconv.Atoi(m[1])
		nodeID, state := m[2], m[3]

		termsSeen[termNum] = true

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

func report(termsSeen map[int]bool, leadersByTerm map[int]map[string]struct{}) {
	// Sort terms numerically for prettier output.
	var terms []int
	for t := range termsSeen {
		terms = append(terms, t)
	}
	sort.Ints(terms)

	for _, term := range terms {
		nodes := leadersByTerm[term]

		switch len(nodes) {
		case 0:
			fmt.Printf("term %-6d  NO LEADER\n", term)
		case 1:
			fmt.Printf("term %-6d  OK\n", term)
		default:
			fmt.Printf("term %-6d  MULTIPLE LEADERS: %v\n", term, keys(nodes))
		}
	}
}

func keys(set map[string]struct{}) []string {
	k := make([]string, 0, len(set))
	for s := range set {
		k = append(k, s)
	}
	sort.Strings(k)
	return k
}
