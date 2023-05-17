package main

import (
	"fmt"
	"sort"
	"sync"
)

func RunPipeline(cmds ...cmd) {
	var wg sync.WaitGroup
	var in chan any
	out := make(chan any)

	wg.Add(len(cmds))

	for _, c := range cmds {
		go func(c cmd, in, out chan any) {
			defer wg.Done()
			c(in, out)
			close(out)
		}(c, in, out)

		in, out = out, make(chan any)
	}

	wg.Wait()
}

func SelectUsers(in, out chan any) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := make(map[uint64]struct{})

	for email := range in {
		email := email.(string)

		wg.Add(1)

		go func() {
			defer wg.Done()

			user := GetUser(email)

			mu.Lock()
			defer mu.Unlock()

			if _, ok := seen[user.ID]; !ok {
				seen[user.ID] = struct{}{}
				out <- user
			}
		}()
	}

	wg.Wait()
}

func SelectMessages(in, out chan any) {
	var wg sync.WaitGroup

	for user := range in {
		batch := make([]User, 0, GetMessagesMaxUsersBatch)
		batch = append(batch, user.(User))

	batchLoop:
		for len(batch) < GetMessagesMaxUsersBatch {
			user, ok := (<-in).(User)
			if !ok {
				break batchLoop
			}
			batch = append(batch, user)
		}

		wg.Add(1)

		go func() {
			defer wg.Done()

			messages, err := GetMessages(batch...)
			if err != nil {
				return
			}

			for _, msg := range messages {
				out <- msg
			}
		}()
	}

	wg.Wait()
}

func CheckSpam(in, out chan any) {
	var wg sync.WaitGroup
	limit := make(chan struct{}, HasSpamMaxAsyncRequests)

	for msg := range in {
		msg := msg.(MsgID)

		wg.Add(1)

		go func() {
			defer wg.Done()

			limit <- struct{}{}

			hasSpam, err := HasSpam(msg)

			<-limit

			if err != nil {
				return
			}

			out <- MsgData{
				ID:      msg,
				HasSpam: hasSpam,
			}
		}()
	}

	wg.Wait()
}

func CombineResults(in, out chan any) {
	results := make([]MsgData, 0)

	for data := range in {
		results = append(results, data.(MsgData))
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].HasSpam != results[j].HasSpam {
			return results[i].HasSpam
		}
		return results[i].ID < results[j].ID
	})

	for _, data := range results {
		out <- fmt.Sprintf("%v %v", data.HasSpam, data.ID)
	}
}
