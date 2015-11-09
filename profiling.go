package main

import (
	"time"

	"github.com/op/go-logging"
)

type Profiler struct {
	request    float64
	impression float64
	click      float64
	latency    float64

	requestNotifier    chan int
	impressionNotifier chan int
	clickNotifier      chan int
	latencyNotifier    chan float64

	configure *Configure
	logger    *logging.Logger
}

func (p *Profiler) OnRequest(timeSpent float64) {
	if p.configure.ProfilerEnable {
		p.requestNotifier <- 1
		p.latencyNotifier <- timeSpent
	}
}

func (p *Profiler) OnImpression() {
	if p.configure.ProfilerEnable {
		p.impressionNotifier <- 1
	}
}

func (p *Profiler) OnClick() {
	if p.configure.ProfilerEnable {
		p.clickNotifier <- 1
	}
}

func NewProfiler(configure *Configure, logger *logging.Logger) *Profiler {
	return &Profiler{
		requestNotifier:    make(chan int, 2048),
		impressionNotifier: make(chan int, 2048),
		clickNotifier:      make(chan int, 2048),
		latencyNotifier:    make(chan float64, 2048),
		configure:          configure,
		logger:             logger,
	}
}

func (p *Profiler) Reset() {
	p.request = 0.0
	p.impression = 0.0
	p.click = 0.0
	p.latency = 0.0
}

func (p *Profiler) Collect() {
	if !p.configure.ProfilerEnable {
		return
	}

	defer func() {
		p.configure.ProfilerEnable = false
	}()
	t := time.NewTimer(time.Duration(p.configure.ProfilerInterval) * time.Second)
	last := time.Now()
	for {
		select {
		case <-t.C:
			now := time.Now()
			timeElaped := now.Sub(last)
			last = now
			p.logger.Info(`Profiling:
	request/s:		%v
	impression/s:		%v
	click/s:		%v
	latency/s:		%v`,
				p.request/timeElaped.Seconds(),
				p.impression/timeElaped.Seconds(),
				p.click/timeElaped.Seconds(),
				p.latency/p.request)
			p.Reset()
			t.Reset(time.Duration(p.configure.ProfilerInterval) * time.Second)
		case <-p.requestNotifier:
			p.request += 1
		case <-p.impressionNotifier:
			p.impression += 1
		case <-p.clickNotifier:
			p.click += 1
		case l := <-p.latencyNotifier:
			p.latency += l
		}
	}
}
