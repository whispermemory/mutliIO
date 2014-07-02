package main

import (
    "os"

	"bufio"
	"fmt"
)

type  choper struct  {
    Fname *string
	Seek int64
	Length int64
	LetoutStartLine bool
}




var  readchan chan *string
var eofchan chan int64
var aviachan chan chan *choper

var nextchan chan bool

func main() {
    initController(3, 1, "/Users/whispermemory/Desktop/territory.conf")
    return 
}


func initController(readerCount int, seekCountThound int,fname string) {
	var eofcount = 0
	var eofseek int64 = 0
	readchan = make(chan *string, readerCount*100)
	eofchan = make(chan int64, 1)
	initReaderChan(readerCount)
	var newSeek int64 = 0
	for i:= 0; i!= readerCount; i++ {
		chopchan := <- aviachan
		chopchan <- &choper{Fname:&fname, Seek:newSeek, Length:int64(1000*seekCountThound), LetoutStartLine:true}
		newSeek += int64(1000*seekCountThound)
	}
	
	println("controller init finish")
	for {
		select {
		case _ = <- readchan :
			//	println(*r)
			
		case  eofseek = <- eofchan:
			eofcount ++
			if eofcount >= readerCount {
				println("read finish")
				fmt.Printf("eof seek %d\n",eofseek)
				return
			}

			println("EOF")


		case s := <-nextchan:
			if  eofseek < newSeek {
				aReader := <-aviachan
				aReader <- &choper {Seek:newSeek, LetoutStartLine:s, Fname:&fname, Length:int64(seekCountThound*1000)}
				println("new seek block")
				newSeek += int64(seekCountThound * 1000)
			} else {
				break
			}

		}
	}

}

func initReaderChan(count int) {
	nextchan = make(chan bool)
	aviachan = make(chan chan *choper, count)
	for i:=0; i!=count; i++ {
		readerChan := make(chan *choper, 1)
		readerChan <- nil
		go reader(readerChan)

	}
}

func reader(c chan *choper) {
	for {
		newTask := <-c
		if newTask == nil {
			aviachan<-c
			continue
		}
		println("seek block start read")
		f, _ := os.Open(*newTask.Fname)
		
		_, err := f.Seek(newTask.Seek, os.SEEK_SET)
		fmt.Printf("seek position : %d", newTask.Seek)
		if err !=nil {
			f.Close()
			eofchan <- newTask.Seek 
			aviachan <- c
			continue
		}
		
		r := bufio.NewReader(f)
		var readSize int64 = 0

		if newTask.LetoutStartLine {
			r.ReadString('\n')
		}
		for {

			if readSize > newTask.Length {
				//				aviachan <- c
				nextchan <- true
				break
			} else if readSize == newTask.Length {
				//				aviachan <- c
				nextchan <- false
				break
			}

			
			line,err := r.ReadString('\n')
			if err!=nil {
				eofchan <- (newTask.Seek + readSize)
				break
			}
			readSize = readSize + int64(len(line)) + 1
			readchan <- &line
			//			aviachan <- c
			println("read string")
		}
		
		f.Close()
		aviachan <- c
		println("seek block finish")
	}

}

