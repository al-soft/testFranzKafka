package main

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	run()
}

func run() {
	seeds := []string{"localhost:9094"}
	// One client can both produce and consume!
	// Consuming can either be direct (no consumer group), or through a group. Below, we use a group.
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.DefaultProduceTopic("beat-output"),
		kgo.ConsumerGroup("my-group-identifier"),
		kgo.ConsumeTopics("beat-output"),
	)
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	ctx := context.Background()

	var records = map[int]string{}
	for i := 0; i <= 2; i++ {
		records[i] = fmt.Sprintf("Hello sky - %d", i)
	}
	sortedKeys := make([]int, 0, len(records))

	for k := range records {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Ints(sortedKeys)

	var wg sync.WaitGroup
	t1 := time.Now()
	for idx := range sortedKeys {
		key := fmt.Sprintf("async-index-%d", idx)
		wg.Add(1)
		record := &kgo.Record{Topic: "beat-output", Key: []byte(key), Value: []byte(records[idx])}
		cl.Produce(ctx, record, func(_ *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				fmt.Printf("record had a produce error: %v\n", err)
			}
		})
		wg.Wait()
	}
	t2 := time.Now()
	fmt.Println(t2.Sub(t1))

	// 1.) Producing a message
	// All record production goes through Produce, and the callback can be used
	// to allow for synchronous or asynchronous production.

	// // Alternatively, ProduceSync exists to synchronously produce a batch of records.
	fmt.Printf("\n\n\n")
	t3 := time.Now()
	var recordmap = []kgo.Record{}
	for idx := range sortedKeys {
		key := fmt.Sprintf("index-%d", idx)
		record := &kgo.Record{Topic: "beat-output", Key: []byte(key), Value: []byte(records[idx])}
		recordmap = append(recordmap, *record)
	}

	if err := cl.ProduceSync(ctx, &recordmap[0], &recordmap[1], &recordmap[2]).FirstErr(); err != nil {
		fmt.Printf("record had a produce error while synchronously producing: %v\n", err)
	}

	t4 := time.Now()
	fmt.Println(t4.Sub(t3))

	// // 2.) Consuming messages from a topic
	// for {
	// 	fetches := cl.PollFetches(ctx)
	// 	if errs := fetches.Errors(); len(errs) > 0 {
	// 		// All errors are retried internally when fetching, but non-retriable errors are
	// 		// returned from polls so that users can notice and take action.
	// 		panic(fmt.Sprint(errs))
	// 	}

	// 	// We can iterate through a record iterator...
	// 	iter := fetches.RecordIter()
	// 	for !iter.Done() {
	// 		record := iter.Next()
	// 		fmt.Println(string(record.Value), "from an iterator!")
	// 	}

	// 	// or a callback function.
	// 	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
	// 		for _, record := range p.Records {
	// 			fmt.Println(string(record.Value), "from range inside a callback!")
	// 		}

	// 		// We can even use a second callback!
	// 		p.EachRecord(func(record *kgo.Record) {
	// 			fmt.Println(string(record.Value), "from a second callback!")
	// 		})
	// 	})
	// }
}
