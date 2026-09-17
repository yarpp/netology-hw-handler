package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Job — задание на обработку одного URL
type Job struct {
	ID  int
	URL string
}

// Result — результат обработки задания
type Result struct {
	Job      Job
	Status   string
	Duration time.Duration
	Err      error
}

// fetch имитирует HTTP-запрос со случайной задержкой от 100 до 1000 мс.
// С вероятностью ~10% имитируется ошибка запроса.
func fetch(url string) (time.Duration, error) {
	delay := time.Duration(100+rand.Intn(900)) * time.Millisecond
	time.Sleep(delay)

	if rand.Intn(10) == 0 {
		return delay, fmt.Errorf("request to %s failed", url)
	}
	return delay, nil
}

// worker — функция-воркер, читает задания из jobs, пишет результаты в results.
// Завершение сигнализируется через wg.Done().
func worker(id int, jobs <-chan Job, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobs {
		start := time.Now()
		_, err := fetch(job.URL)
		elapsed := time.Since(start)

		status := "обработан"
		if err != nil {
			status = "ошибка"
		}
		results <- Result{
			Job:      job,
			Status:   status,
			Duration: elapsed,
			Err:      err,
		}
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	urls := []string{
		"https://vkvideo.ru",
		"https://go.dev",
		"https://golang.org/doc",
		"https://vk.ru",
		"https://github.com",
		"https://habr.com",
		"https://stackoverflow.com",
		"https://google.com",
		"https://ya.ru",
		"https://wikipedia.org",
		"https://npo-echelon.ru",
		"https://www.kaspersky.ru",
		"https://app.diagrams.net",
		"https://netology.ru",
		"https://ptsecurity.com",
	}

	const numWorkers = 5

	jobs := make(chan Job, len(urls))       // буферизован, чтобы не блокировать отправку
	results := make(chan Result, len(urls)) // буферизован, чтобы воркеры не залипали

	var wg sync.WaitGroup

	// Fan-out: запускаем пул воркеров
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go worker(i, jobs, results, &wg)
	}

	// Отправляем задания
	for i, u := range urls {
		jobs <- Job{ID: i + 1, URL: u}
	}
	close(jobs)

	// Fan-in: отдельная горутина ждёт завершения всех воркеров и закрывает results
	go func() {
		wg.Wait()
		close(results)
	}()

	// Собираем результаты в главной горутине
	var collected []Result
	for r := range results {
		collected = append(collected, r)
	}

	// Вывод отчёта
	printReport(collected)
}

func printReport(results []Result) {
	fmt.Println("ОТЧЁТ ОБ ОБРАБОТКЕ URL")
	fmt.Printf("%-4s %-35s %-12s %-12s\n", "ID", "URL", "СТАТУС", "ВРЕМЯ")

	var total time.Duration
	success := 0
	for _, r := range results {
		fmt.Printf("%-4d %-35s %-12s %-12s\n",
			r.Job.ID, r.Job.URL, r.Status, r.Duration.Round(time.Millisecond))
		total += r.Duration
		if r.Err == nil {
			success++
		}
	}

	fmt.Printf("Всего задач:        %d\n", len(results))
	fmt.Printf("Успешных:           %d\n", success)
	fmt.Printf("С ошибкой:          %d\n", len(results)-success)
	if len(results) > 0 {
		avg := total / time.Duration(len(results))
		fmt.Printf("Среднее время:      %s\n", avg.Round(time.Millisecond))
	}
	fmt.Printf("Суммарное время:    %s\n", total.Round(time.Millisecond))
}
