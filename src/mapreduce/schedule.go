package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	var mutex = &sync.Mutex{}
	c := sync.NewCond(mutex)

	// This queue is to ensure no leakage when recv and append workers.
	queue := make([]string, 0)
	quit := make(chan int)
	var wg sync.WaitGroup

	go func() {
		for {
			select {
			case srv := <-registerChan:
				mutex.Lock()
				queue = append(queue, srv)
				c.Broadcast()
				mutex.Unlock()
			case <-quit:
				break
			}
		}
	}()

	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		go func(i int) {
			for {
				defer wg.Done()
				args := DoTaskArgs{
					JobName:       jobName,
					Phase:         phase,
					TaskNumber:    i,
					NumOtherPhase: n_other}

				if phase == mapPhase {
					args.File = mapFiles[i]
				}
				c.L.Lock()
				// wait for queue to be size >= 1
				for len(queue) == 0 {
					c.Wait()
				}
				endpoint := queue[0]
				queue = queue[1:]
				c.L.Unlock()
				ok := call(endpoint, "Worker.DoTask", &args, new(struct{}))
				if ok == false {
					wg.Add(1)
					// rerun the whole task
				} else {
					mutex.Lock()
					queue = append(queue, endpoint)
					c.Broadcast()
					mutex.Unlock()
					break
				}
			}
		}(i)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	wg.Wait()
	quit <- 0
	fmt.Printf("Schedule: %v phase done\n", phase)
}
