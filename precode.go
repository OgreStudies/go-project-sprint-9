package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Generator генерирует последовательность чисел 1,2,3 и т.д. и
// отправляет их в канал ch. При этом после записи в канал для каждого числа
// вызывается функция fn. Она служит для подсчёта количества и суммы
// сгенерированных чисел.
func Generator(ctx context.Context, ch chan<- int64, fn func(int64)) {

	cnt := int64(1) //начальное значение счётчика 1

	if ch == nil {
		log.Fatalf("Generator: Канал ch не может быть nil")
		return
	}

	defer close(ch) //закрыть выходной канал ch при завершении функции
	for {
		select {
		case <-ctx.Done(): //Ожидание закрытия контекста
			//При закрытии контекста  и выйти из фун
			return
		case ch <- cnt: //Отправка нового значения в канал ch
			//Обработка переданного значения
			fn(cnt)
			//Увеличение счётчика на 1 для передачи значения на следующей итерации
			cnt++
		}
	}
}

// Worker читает число из канала in и пишет его в канал out.
func Worker(in <-chan int64, out chan<- int64) {

	if out == nil {
		log.Fatalf("Worker: Канал out не может быть nil")
		return
	}
	//Призавершении функции закрыть канал out
	defer close(out)

	if in == nil {
		log.Fatalf("Worker: Канал in не может быть nil")
		return
	}

	//читаем значения из входного канала до закрытия
	for v := range in {
		out <- v
		//Уснуть на одну милисекнду перед следующей попыткой
		time.Sleep(time.Millisecond)
	}
}

func main() {
	chIn := make(chan int64)
	//Контекст с закрытием через секунду
	ctx, cansel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	defer cansel()

	// для проверки будем считать количество и сумму отправленных чисел
	var inputSum int64   // сумма сгенерированных чисел
	var inputCount int64 // количество сгенерированных чисел

	// генерируем числа, считая параллельно их количество и сумму
	go Generator(ctx, chIn, func(i int64) {
		atomic.AddInt64(&inputSum, i)
		atomic.AddInt64(&inputCount, 1)
	})

	const NumOut = 5 // количество обрабатывающих горутин и каналов
	// outs — слайс каналов, куда будут записываться числа из chIn
	outs := make([]chan int64, NumOut)
	for i := 0; i < NumOut; i++ {
		// создаём каналы и для каждого из них вызываем горутину Worker
		outs[i] = make(chan int64)
		go Worker(chIn, outs[i])
	}

	// amounts — слайс, в который собирается статистика по горутинам
	amounts := make([]int64, NumOut)
	// chOut — канал, в который будут отправляться числа из горутин `outs[i]`
	chOut := make(chan int64, NumOut)

	var wg sync.WaitGroup

	// 4. Собираем числа из каналов outs
	for idx, co := range outs {
		wg.Add(1)
		go func(in <-chan int64, i int64) {
			for val := range in {
				amounts[i]++
				chOut <- val
			}
			wg.Done()
		}(co, int64(idx))
	}

	go func() {
		// ждём завершения работы всех горутин для outs
		wg.Wait()
		// закрываем результирующий канал
		close(chOut)
	}()

	var count int64 // количество чисел результирующего канала
	var sum int64   // сумма чисел результирующего канала

	// 5. Читаем числа из результирующего канала
	for resVal := range chOut {
		sum += resVal
		count++
	}

	fmt.Println("Количество чисел", inputCount, count)
	fmt.Println("Сумма чисел", inputSum, sum)
	fmt.Println("Разбивка по каналам", amounts)

	// проверка результатов
	if inputSum != sum {
		log.Fatalf("Ошибка: суммы чисел не равны: %d != %d\n", inputSum, sum)
	}
	if inputCount != count {
		log.Fatalf("Ошибка: количество чисел не равно: %d != %d\n", inputCount, count)
	}
	for _, v := range amounts {
		inputCount -= v
	}
	if inputCount != 0 {
		log.Fatalf("Ошибка: разделение чисел по каналам неверное\n")
	}
}
