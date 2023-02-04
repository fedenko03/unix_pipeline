package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var concurrency = 6

// ExecutePipeline executes pipeline of jobs using specified concurrency.
func ExecutePipeline(jobs ...job) {
	// create WaitGroup to wait for all workers to complete
	wg := &sync.WaitGroup{}
	// create input channel for pipeline
	in := make(chan interface{})

	for _, job := range jobs {
		wg.Add(1)

		// create output channel for the job
		out := make(chan interface{})
		go jobWorker(job, in, out, wg)
		in = out
	}
	// wait for all workers to complete
	wg.Wait()
}

// jobWorker executes a job and closes output channel after job completion.
func jobWorker(job job, in, out chan interface{}, wg *sync.WaitGroup) {
	// decrement the WaitGroup counter when the worker returns
	defer wg.Done()
	// close the output channel after job completion
	defer close(out)

	job(in, out)
}

// SingleHash performs single hash calculation for input data and sends the result to output channel.
func SingleHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}

	for i := range in {
		wg.Add(1)
		go singleHashJob(i, out, wg, mu)
	}
	wg.Wait()
}

// singleHashJob performs single hash calculation for a piece of input data.
func singleHashJob(in interface{}, out chan interface{}, wg *sync.WaitGroup, mu *sync.Mutex) {
	// decrement the WaitGroup counter when the worker returns
	defer wg.Done()
	// convert input data to string
	data := strconv.Itoa(in.(int))

	// lock the mutex to synchronize access to shared data
	mu.Lock()
	// calculate md5 hash of the input data
	md5Data := DataSignerMd5(data)
	// unlock the mutex
	mu.Unlock()

	// create channel for crc32 hash calculation result
	crc32Chan := make(chan string)
	// start a worker for crc32 hash calculation
	go asyncCrc32Signer(data, crc32Chan)

	// receive crc32 hash calculation result
	crc32Data := <-crc32Chan
	// calculate crc32 hash of the md5 hash
	crc32Md5Data := DataSignerCrc32(md5Data)

	out <- crc32Data + "~" + crc32Md5Data
}

// asyncCrc32Signer computes the crc32 checksum for a given data string
// and writes the result to the provided output channel.
func asyncCrc32Signer(data string, out chan string) {
	// Compute the crc32 checksum of the data string
	out <- DataSignerCrc32(data)
}

// MultiHash performs the multi-hash operation on input data. It processes
// the input data in parallel and writes the result to the provided output channel.
func MultiHash(in, out chan interface{}) {
	const TH int = 6
	wg := &sync.WaitGroup{}

	for i := range in {
		wg.Add(1)
		go multiHashJob(i.(string), out, TH, wg)
	}
	wg.Wait()
}

// multiHashJob performs the multi-hash operation on a single piece of input data.
func multiHashJob(in string, out chan interface{}, th int, wg *sync.WaitGroup) {
	defer wg.Done()

	mu := &sync.Mutex{}
	jobWg := &sync.WaitGroup{}
	combinedChunks := make([]string, th)

	for i := 0; i < th; i++ {
		jobWg.Add(1)
		data := strconv.Itoa(i) + in

		go func(acc []string, index int, data string, jobWg *sync.WaitGroup, mu *sync.Mutex) {
			defer jobWg.Done()
			data = DataSignerCrc32(data)

			mu.Lock()
			acc[index] = data
			mu.Unlock()
		}(combinedChunks, i, data, jobWg, mu)
	}

	jobWg.Wait()
	out <- strings.Join(combinedChunks, "")
}

// CombineResults combines the results of multiple inputs into a single output string.
func CombineResults(in, out chan interface{}) {
	var result []string

	for i := range in {
		result = append(result, i.(string))
	}

	sort.Strings(result)
	out <- strings.Join(result, "_")
}

func main() {
	fmt.Println("Ready")
}
