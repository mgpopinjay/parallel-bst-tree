package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"text/scanner"
	"time"
)

/*****************************************************************************************/

type Jobs struct {
	tree    *Node
	tree_id int
	name    string
}

type InputArgs struct {
	hash_workers *int
	data_workers *int
	comp_workers *int
	input_file   *string
	run_mode     int
}

type WorkerArgs struct {
	sequential           bool
	eq_hash_data_workers bool
	hash_only            bool
	map_only             bool
	special_case         bool
}

type Node struct {
	value int
	left  *Node
	right *Node
}

type HashTreePair struct {
	hash    int
	tree_id int
}

// Pre-process input args
func args_parser() InputArgs {
	f := flag.String("input", "", "a string")
	h := flag.Int("hash-workers", 1, "an int")
	d := flag.Int("data-workers", 0, "an int")
	c := flag.Int("comp-workers", 0, "an int")
	flag.Parse()

	args := InputArgs{hash_workers: h, data_workers: d, comp_workers: c, input_file: f}
	return args
}

// Build worker args pack
func load_args(args InputArgs) WorkerArgs {
	var hash_only, map_only, eq_hash_data_workers, sequential, special_case bool

	if *args.hash_workers == 1 {
		sequential = true
	} else {
		sequential = false
	}

	if *args.data_workers == 0 {
		hash_only = true
	} else {
		hash_only = false
	}

	if *args.comp_workers == 0 {
		map_only = true
	} else {
		map_only = false
	}

	// Equal number of hash and data workers
	if !sequential && (*args.hash_workers == *args.data_workers) {
		eq_hash_data_workers = true
	} else {
		eq_hash_data_workers = false
	}

	// Case: hash-workers > data-workers > 1
	if !sequential && *args.data_workers > 1 && !eq_hash_data_workers {
		special_case = true
	} else {
		special_case = false
	}

	worker_args := WorkerArgs{map_only: map_only, hash_only: hash_only,
		eq_hash_data_workers: eq_hash_data_workers,
		sequential:           sequential, special_case: special_case}

	return worker_args
}

// Hash-making method
func (tree *Node) computeHash() int {
	var hash int = 1
	var result []int
	var new_val int
	for _, value := range tree.in_order_traversal(result) {
		new_val = value + 2
		hash = (hash*new_val + new_val) % 1000
	}
	//fmt.Println("hash:", hash)
	return hash
}

// In-order tree traversal Method for hash computation
func (tree *Node) in_order_traversal(result []int) []int {
	if tree != nil {
		result = tree.right.in_order_traversal(result)
		result = append(result, tree.value)
		result = tree.left.in_order_traversal(result)
	}
	return result
}

// Node Insertion Method
func (n *Node) Insert(value int) {
	if value > n.value {
		if n.right == nil {
			n.right = &Node{value: value}
		} else {
			n.right.Insert(value)
		}
	} else {
		if n.left == nil {
			n.left = &Node{value: value}
		} else {
			n.left.Insert(value)
		}
	}
}

// Builds BST from input text file
func buildTrees(file *string, all_trees *[]*Node) {
	f, err := os.Open(*file)
	if err != nil {
		log.Fatal(err)
	}

	f_scan := bufio.NewScanner(f)

	for f_scan.Scan() {
		var s scanner.Scanner
		s.Init(strings.NewReader(f_scan.Text()))
		var new_line bool = true
		var tree *Node

		for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
			tokenized, _ := strconv.Atoi(s.TokenText())
			if new_line {
				new_line = false
				tree = &Node{value: tokenized}
			} else {
				tree.Insert(tokenized)
			}
		}
		*all_trees = append(*all_trees, tree)
	}
	f.Close()
}

// Compare Trees

func sameTrees(a *Node, b *Node) bool {
	var a_flat, b_flat []int
	a_flat, b_flat = a.in_order_traversal(a_flat), b.in_order_traversal(b_flat)

	for i := 0; i < len(a_flat); i++ {
		if a_flat[i] != b_flat[i] {
			return false
		}
	}
	return true
}

// Parallel Hash computation with map update (non-special case)

func parallel_hash(job_ch <-chan Jobs, hash_map *map[int][]int,
	wg *sync.WaitGroup, mutex *sync.Mutex, queue chan<- HashTreePair, parallel_update bool) {

	if parallel_update {
		//fmt.Println("Workers Map update using Mutex Lock")
		//start := time.Now()
		for job := range job_ch {
			var hash int = job.tree.computeHash()
			mutex.Lock() // Lock hashmap while updating
			(*hash_map)[hash] = append((*hash_map)[hash], job.tree_id)
			mutex.Unlock()
			wg.Done() // decrement WaitGroup by 1
		}
		//elapsed := time.Since(start)
		//fmt.Printf("One Mutex lock per thread update  %s\n", elapsed)
	}
	// One central manager channel aggregates all hash-pairs for map update
	if !parallel_update {
		//fmt.Println("One-Channel Map Update")
		for job := range job_ch {
			var hash int = job.tree.computeHash()
			hash_pair := HashTreePair{hash: hash, tree_id: job.tree_id}
			queue <- hash_pair
			wg.Done() // decrement WaitGroup by 1
		}
	}
}

// Parallel grouping with Mutex Lock

func mutex_map(queue <-chan HashTreePair, hash_map *map[int][]int,
	wg *sync.WaitGroup, mutex *sync.Mutex) {
	for job := range queue {
		mutex.Lock()
		(*hash_map)[job.hash] = append((*hash_map)[job.hash], job.tree_id)
		mutex.Unlock()
		wg.Done()
	}
}

// Driver function used in main()

func run_all(all_trees *[]*Node, hash_map *map[int][]int, same_trees *map[int][]int, args InputArgs, worker_args WorkerArgs) {

	var wg sync.WaitGroup
	var wg_data sync.WaitGroup // for hash-workers > data-workers > 1

	var mutex sync.Mutex
	var mutex_data sync.Mutex // for hash-workers > data-workers > 1

	buildTrees(args.input_file, all_trees)

	job_ch := make(chan Jobs, len(*all_trees))         // one hash channel for each tree
	map_ch := make(chan HashTreePair, len(*all_trees)) // one map update channel for each tree
	visited_trees := make(map[int]VisitRecord)

	//fmt.Println("all_trees", &all_trees)

	/*** Part I: Sequential Hash Tree Computation, Grouping and Comparison ***/

	if worker_args.sequential {
		// Sequential Hashing-only (1/0/0)
		if worker_args.hash_only && worker_args.map_only {
			start := time.Now()

			for _, tree := range *all_trees {
				tree.computeHash()
			}
			hashTime := time.Since(start)
			fmt.Printf("hashTime (1/0/0 sequential) %s\n", hashTime)
			return
		}
		// Sequential Hashing + Grouping (1/1/0)
		if !worker_args.hash_only && worker_args.map_only {
			group_start := time.Now()

			for tree_id, tree := range *all_trees {
				var hash int = tree.computeHash()
				(*hash_map)[hash] = append((*hash_map)[hash], tree_id)
			}
			hashGroupTime := time.Since(group_start)
			fmt.Printf("hashGroupTime (1/1/0 sequential) %s\n", hashGroupTime)
		}

		// Sequential Hashing + Grouping + Comparison (1/1/1)
		if !worker_args.hash_only && !worker_args.map_only {
			group_start := time.Now()
			for tree_id, tree := range *all_trees {
				var hash int = tree.computeHash()
				(*hash_map)[hash] = append((*hash_map)[hash], tree_id)
			}
			hashGroupTime := time.Since(group_start)
			fmt.Printf("hashGroupTime (1/1/_ sequential) %s\n", hashGroupTime)

			comp_start := time.Now()

			compare_trees(all_trees, hash_map, same_trees)

			compareTreeTime := time.Since(comp_start)
			fmt.Printf("compareTreeTime (1/1/1 sequential) %s\n", compareTreeTime)

			return
		}
	}

	/*** Part II: Parallel Hash Tree Computation and Grouping  ***/

	if !worker_args.sequential {

		// Parallel hashing
		wg.Add(len(*all_trees)) // Add one WaitGroup per tree for parallel_hash gorountines
		fmt.Println("Parallel Map Update", worker_args.eq_hash_data_workers, "on", len(*all_trees), "trees")

		p_hash_start := time.Now()

		// Send trees into job_ch channel for parallel_hash goroutines
		for i, tree := range *all_trees {
			job_ch <- Jobs{tree: tree, name: fmt.Sprintf("JobID::%d", i), tree_id: i}
		}

		// Launch goroutines per specified number of hash workers
		for id := 0; id < *args.hash_workers; id++ {
			go parallel_hash(job_ch, hash_map,
				&wg, &mutex, map_ch, worker_args.eq_hash_data_workers)
		}

		close(job_ch) // Close channel for hashing jobs

		wg.Wait()

		//close(map_ch) // Close channel for hash map update

		hashTime := time.Since(p_hash_start)
		fmt.Printf("hashTime (i/0/0 parallel) %s\n", hashTime)

		close(map_ch) // Close channel for hash map update

		// Exit if ONLY hash-workers is specified
		if worker_args.hash_only {
			return
		}

		// KEEP???
		//"Central Manager" channel updates hashmap
		//for pair := range map_ch {
		//	(*hash_map)[pair.hash] = append((*hash_map)[pair.hash], pair.tree_id)
		//}

		// Case: data-workers = 1 or same as hash-workers
		if !worker_args.hash_only && (*args.data_workers == 1 || worker_args.eq_hash_data_workers) {
			for pair := range map_ch {
				(*hash_map)[pair.hash] = append((*hash_map)[pair.hash], pair.tree_id)
			}
			hashGroupTime := time.Since(p_hash_start)
			fmt.Printf("hashGroupTime (i/1/0 i/i/0) %s\n", hashGroupTime)
		}

		//Extra Credit: hash-workers > data-workers > 1
		if *args.data_workers != 1 && !worker_args.eq_hash_data_workers {
			//if !worker_args.eq_hash_data_workers && *args.data_workers > 1 {
			wg_data.Add(len(*all_trees))
			start := time.Now()

			for id := 0; id < *args.data_workers; id++ {
				go mutex_map(map_ch, hash_map,
					&wg_data, &mutex_data)
			}
			wg_data.Wait()
			elapsed := time.Since(start) + hashTime // include hashing time
			fmt.Printf("hashGroupTime (i/j/0 parallel) %s\n", elapsed)
		}

		// Print Hash Groups of size > 1
		if *args.data_workers > 0 {
			for hash, tree_id := range *hash_map {
				if len(tree_id) > 1 {
					fmt.Printf("%d:", hash)
					for _, id := range tree_id {
						fmt.Printf(" %d", id)
					}
					fmt.Println()
				}
			}
		}

	}

	// Terminate upon tree grouping if no comp-worker is specified.
	if worker_args.map_only {
		return
	}

	/*** Part III: Parallel Tree Comparison  ***/

	if *args.comp_workers == 1 {
		comp_start := time.Now()
		compare_trees(all_trees, hash_map, same_trees)
		compareTreeTime := time.Since(comp_start)
		fmt.Printf("compareTreeTime (n/n/1 sequential) %s\n", compareTreeTime)
	} else {
		comp_start := time.Now()
		compare_trees_parallel(all_trees, hash_map, same_trees, *args.comp_workers, &visited_trees)
		compareTreeTime := time.Since(comp_start)
		fmt.Printf("compareTreeTime (n/n/n parallel) %s\n", compareTreeTime)
	}

	// Print Tree Groups
	for group_id, tree_id := range *same_trees {
		if len(tree_id) > 1 {
			fmt.Printf("group %d:", group_id)
			for _, id := range tree_id {
				fmt.Printf(" %d", id)
			}
			fmt.Println()
		}
	}
}

func main() {
	var all_trees []*Node
	hash_map := make(map[int][]int)
	same_trees := make(map[int][]int)

	var args InputArgs = args_parser()

	fmt.Println("hash workers=", *args.hash_workers, "data workers=", *args.data_workers, "comp workers=",
		*args.comp_workers, "input file=", *args.input_file)
	fmt.Println("Base num of cores for thread assignment available:", runtime.GOMAXPROCS(-1))

	worker_args := load_args(args)

	run_all(&all_trees, &hash_map, &same_trees, args, worker_args)
}

type CompJobs struct {
	space int
	pairs []TreePair
	mutex *sync.Mutex
}

// Method for adding tree pair to job queue
func (job *CompJobs) Insert(pair TreePair) bool {
	job.mutex.Lock()
	defer job.mutex.Unlock()

	if job.space > len(job.pairs) { // Check for room in buffer
		job.pairs = append(job.pairs, pair)
		return true
	} else {
		return false
	}
}

// Method for poping the oldest tree pair from job queue
func (job *CompJobs) Remove() (TreePair, error) {
	job.mutex.Lock()
	defer job.mutex.Unlock()
	var pair TreePair

	if len(job.pairs) > 0 {
		pair := job.pairs[0]
		job.pairs = job.pairs[1:]
		return pair, nil
	} else {
		return pair, errors.New("Empty")
	}

}

// CreateQueue creates an empty queue with desired capacity
func MakeCompJobs(capacity int, mutex *sync.Mutex) *CompJobs {
	return &CompJobs{
		space: capacity,
		pairs: make([]TreePair, 0, capacity),
		mutex: mutex,
	}
}

type TreePair struct {
	group_id int
	id_a     int
	id_b     int
}

type VisitRecord struct {
	visited  bool
	group_id int
}

func register_same_trees(a_id int, b_id int,
	mutex *sync.Mutex, visited_trees *map[int]VisitRecord) {

	var id int
	var a_checked, b_checked VisitRecord
	var a_ok, b_ok bool

	mutex.Lock()
	defer mutex.Unlock()

	a_checked, a_ok = (*visited_trees)[a_id]
	b_checked, b_ok = (*visited_trees)[b_id]

	if !a_ok && !b_ok {

		if a_id < b_id {
			id = a_id
		} else {
			id = b_id
		}
		visited := VisitRecord{visited: true, group_id: id}
		(*visited_trees)[a_id] = visited
		(*visited_trees)[b_id] = visited
		return
	}
	if a_ok && b_ok {
		if a_id < b_id {
			id = a_checked.group_id
		} else {
			id = b_checked.group_id
		}
		visited := VisitRecord{visited: true, group_id: id}
		(*visited_trees)[b_id] = visited
		(*visited_trees)[a_id] = visited
		return
	}
	if a_ok && !b_ok {
		(*visited_trees)[b_id] = a_checked
		return
	}
	if b_ok && !a_ok {
		(*visited_trees)[a_id] = b_checked
		return
	}
	return
}

// ???

func comp_worker_run(job *CompJobs, all_trees *[]*Node,
	mutex_visited *sync.Mutex, visited_trees *map[int]VisitRecord,
	continue_working <-chan int, wg *sync.WaitGroup) {
	for {
		var pair TreePair
		var err error

		// Prevent work removal from an empty buffer
		for {
			_, ok := <-continue_working
			if !ok {
				return
			}
			pair, err = job.Remove()
			if err == nil {
				break
			}
			time.Sleep(1 * time.Millisecond)
		}
		var curr_node, next_node *Node = (*all_trees)[pair.id_a], (*all_trees)[pair.id_b]

		if sameTrees(curr_node, next_node) {
			register_same_trees(pair.id_a, pair.id_b,
				mutex_visited, visited_trees)
		}
		wg.Done()
	}
}

// ???

func compare_trees_parallel(all_trees *[]*Node, hash_map *map[int][]int,
	same_trees *map[int][]int, comp_workers int,
	visited_trees *map[int]VisitRecord) {

	var mutex_comp sync.Mutex
	var mutex_visited sync.Mutex
	var wg sync.WaitGroup

	comp_jobs := MakeCompJobs(comp_workers, &mutex_comp) //pointer!!

	// REPLACE this with signal-only struct {} ???
	continue_working := make(chan int)

	// Launch tree comparison goroutines
	for i := 0; i < comp_workers; i++ {
		go comp_worker_run(comp_jobs, all_trees, &mutex_visited, visited_trees, continue_working, &wg)
	}

	// Feed tree pairs into pipeline channel
	for group_id, tree_ids := range *hash_map { //flatten out elements in hashmap
		if len(tree_ids) > 1 {
			for i := 0; i < len(tree_ids); i++ {
				for j := i + 1; j < (len(tree_ids)); j++ {
					pair := TreePair{group_id: group_id, id_a: tree_ids[i], id_b: tree_ids[j]}
					wg.Add(1)

					// Prevent main thread from inserting work into a full buffer
					for !comp_jobs.Insert(pair) {
						//sleep the main thread until queue is freed
						time.Sleep(1 * time.Millisecond)
					}
					continue_working <- 1
				}
			}
		}
	}
	wg.Wait() // Let goroutines synchronize

	close(continue_working)

	final_map := make(map[int]int)

	var count int = 0
	var exists bool
	var id int
	for key, value := range *visited_trees {
		id, exists = final_map[value.group_id]
		if !exists {
			final_map[value.group_id] = count
			id = count
			count++
		}
		(*same_trees)[id] = append((*same_trees)[id], key)
	}
}

// ???

func compare_trees(all_trees *[]*Node, hash_map *map[int][]int,
	same_trees *map[int][]int) {

	var tree_group int = -1

	for _, tree_ids := range *hash_map {
		this_group_visited := make(map[int]bool)
		if len(tree_ids) > 1 {
			//fmt.Printf("Compare values in key: %d\n", hash, tree_ids)
			//fmt.Println(hash, tree_ids)

			for i := 0; i < len(tree_ids); i++ {
				if !this_group_visited[i] {

					//node hasn't been visited yet, create new group in tree
					tree_group++
					(*same_trees)[tree_group] = append((*same_trees)[tree_group], tree_ids[i])
					//fmt.Println("same_trees:", *same_trees)

					var node *Node = (*all_trees)[tree_ids[i]]

					this_group_visited[i] = true
					for j := i + 1; j < len(tree_ids); j++ {
						if !this_group_visited[j] {
							//next node hasn't been visited, compare with node
							var next_node *Node = (*all_trees)[tree_ids[j]]
							var equal bool = sameTrees(node, next_node)
							if equal {
								(*same_trees)[tree_group] = append((*same_trees)[tree_group], tree_ids[j])
								this_group_visited[j] = true //grouped nextnode, remove it from iterations
							}
						}
					}
				}
			}
		} // else no need to print groups with only one tree
		//fmt.Println("same_trees:", *same_trees)
	}
	//fmt.Println("same_trees:", *same_trees)
}
