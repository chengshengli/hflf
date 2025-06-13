package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
)

// Tick represents a single trade tick parsed from .tick file
// only required fields are kept
// time is stored as epoch milliseconds

// service state is persisted in a .state file

// KLine is 1 minute OHLCV bar

// Implementation will maintain simple structures

type Tick struct {
	EpochMs  int64
	Price    float64
	TotalVol int64
}

type KLine struct {
	StartTimeMs int64
	Open        float64
	High        float64
	Low         float64
	Close       float64
	Volume      int64
}

type ServiceState struct {
	TickFileLastReadOffset  int64 `json:"TickFileLastReadOffset"`
	LastCompletedBarEpochMs int64 `json:"LastCompletedBarEpochMs"`
	LastTotalVolume         int64 `json:"LastTotalVolume"`

	currentBar *KLine
}

var (
	inputPath  string
	outDir     string
	debugPrint bool

	mutex   sync.Mutex
	eastern *time.Location
)

func debug(format string, a ...interface{}) {
	if debugPrint {
		fmt.Printf(format+"\n", a...)
	}
}

func init() {
	flag.StringVar(&inputPath, "input", "", "path to .tick file")
	flag.StringVar(&outDir, "out-dir", "", "output directory")
	flag.BoolVar(&debugPrint, "debug-print", false, "enable debug logs")
}

func main() {
	flag.Parse()
	if inputPath == "" || outDir == "" {
		fmt.Println("-input and -out-dir required")
		os.Exit(1)
	}
	if err := os.MkdirAll(outDir, 0755); err != nil {
		fmt.Printf("failed to create out dir: %v\n", err)
		os.Exit(1)
	}
	var err error
	eastern, err = time.LoadLocation("America/New_York")
	if err != nil {
		fmt.Printf("load tz: %v\n", err)
		os.Exit(1)
	}
	state, err := loadOrBootstrapState()
	if err != nil {
		fmt.Printf("cannot load state: %v\n", err)
		os.Exit(1)
	}

	ctx, stop := signalContext()
	defer stop()

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go watchFile(ctx, wg, state)

	<-ctx.Done()
	debug("shutting down")
	mutex.Lock()
	finalizeCurrentBar(state)
	flushCurrentBar(state)
	saveState(state)
	mutex.Unlock()
	wg.Wait()
}

func signalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		cancel()
	}()
	return ctx, cancel
}

func watchFile(ctx context.Context, wg *sync.WaitGroup, state *ServiceState) {
	defer wg.Done()
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		fmt.Printf("watcher error: %v\n", err)
		return
	}
	defer watcher.Close()
	dir := filepath.Dir(inputPath)
	watcher.Add(dir)
	debounce := time.NewTimer(time.Hour)
	debounce.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-watcher.Events:
			if ev.Op&(fsnotify.Write|fsnotify.Create) > 0 && ev.Name == inputPath {
				debounce.Reset(500 * time.Millisecond)
			}
		case <-debounce.C:
			processNewTicks(state)
		case err := <-watcher.Errors:
			debug("watcher error: %v", err)
		}
	}
}

func processNewTicks(state *ServiceState) {
	mutex.Lock()
	defer mutex.Unlock()
	f, err := os.Open(inputPath)
	if err != nil {
		debug("open tick: %v", err)
		return
	}
	defer f.Close()
	if _, err := f.Seek(state.TickFileLastReadOffset, io.SeekStart); err != nil {
		debug("seek: %v", err)
		return
	}
	reader := bufio.NewReader(f)
	for {
		line, err := reader.ReadString('\n')
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			debug("read tick: %v", err)
			break
		}
		state.TickFileLastReadOffset += int64(len(line))
		if strings.HasPrefix(line, "[") && strings.Contains(line, ",") {
			tick, err := parseTick(line)
			if err == nil {
				processSingleTick(state, tick)
			}
		}
	}
	flushCurrentBar(state)
	saveState(state)
}

func parseTick(line string) (Tick, error) {
	line = strings.TrimSpace(line)
	line = strings.Trim(line, "[]")
	parts := strings.Split(line, ",")
	if len(parts) < 7 {
		return Tick{}, fmt.Errorf("bad tick line")
	}
	price, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return Tick{}, err
	}
	tot, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return Tick{}, err
	}
	t, err := time.Parse(time.RFC3339, parts[4])
	if err != nil {
		return Tick{}, err
	}
	return Tick{EpochMs: t.UnixMilli(), Price: price, TotalVol: tot}, nil
}

func processSingleTick(state *ServiceState, tick Tick) {
	volumeDelta := tick.TotalVol - state.LastTotalVolume
	if volumeDelta < 0 {
		volumeDelta = tick.TotalVol
	}
	state.LastTotalVolume = tick.TotalVol

	minute := tick.EpochMs - (tick.EpochMs % 60000)
	if state.currentBar == nil || minute > state.currentBar.StartTimeMs {
		if state.currentBar != nil {
			finalizeCurrentBar(state)
		}
		state.currentBar = &KLine{
			StartTimeMs: minute,
			Open:        tick.Price,
			High:        tick.Price,
			Low:         tick.Price,
			Close:       tick.Price,
			Volume:      volumeDelta,
		}
		return
	}
	b := state.currentBar
	if tick.Price > b.High {
		b.High = tick.Price
	}
	if tick.Price < b.Low {
		b.Low = tick.Price
	}
	b.Close = tick.Price
	b.Volume += volumeDelta
}

func finalizeCurrentBar(state *ServiceState) {
	if state.currentBar == nil {
		return
	}
	path := filepath.Join(outDir, strings.TrimSuffix(filepath.Base(inputPath), filepath.Ext(inputPath))+".1m")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		debug("open history: %v", err)
		return
	}
	defer f.Close()
	if fi, _ := f.Stat(); fi.Size() == 0 {
		fmt.Fprintln(f, "[EasternTime,High,Low,Open,Close,Volume,Epoch]")
	}
	fmt.Fprintf(f, "[%s,%.6f,%.6f,%.6f,%.6f,%d,%d]\n", time.UnixMilli(state.currentBar.StartTimeMs).In(eastern).Format("2006-01-02T15:04:05"),
		state.currentBar.High, state.currentBar.Low, state.currentBar.Open, state.currentBar.Close, state.currentBar.Volume, state.currentBar.StartTimeMs)
	state.LastCompletedBarEpochMs = state.currentBar.StartTimeMs
	state.currentBar = nil
}

func flushCurrentBar(state *ServiceState) {
	if state.currentBar == nil {
		return
	}
	path := filepath.Join(outDir, strings.TrimSuffix(filepath.Base(inputPath), filepath.Ext(inputPath))+".1m.current")
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		debug("open current: %v", err)
		return
	}
	fmt.Fprintln(f, "[EasternTime,High,Low,Open,Close,Volume,Epoch]")
	fmt.Fprintf(f, "[%s,%.6f,%.6f,%.6f,%.6f,%d,%d]\n", time.UnixMilli(state.currentBar.StartTimeMs).In(eastern).Format("2006-01-02T15:04:05"),
		state.currentBar.High, state.currentBar.Low, state.currentBar.Open, state.currentBar.Close, state.currentBar.Volume, state.currentBar.StartTimeMs)
	f.Close()
	os.Rename(tmp, path)
}

func saveState(state *ServiceState) {
	path := filepath.Join(outDir, strings.TrimSuffix(filepath.Base(inputPath), filepath.Ext(inputPath))+".state")
	tmp := path + ".tmp"
	out := struct {
		TickFileLastReadOffset  int64 `json:"TickFileLastReadOffset"`
		LastCompletedBarEpochMs int64 `json:"LastCompletedBarEpochMs"`
		LastTotalVolume         int64 `json:"LastTotalVolume"`
	}{state.TickFileLastReadOffset, state.LastCompletedBarEpochMs, state.LastTotalVolume}
	data, _ := json.Marshal(out)
	os.WriteFile(tmp, data, 0644)
	os.Rename(tmp, path)
}

func loadOrBootstrapState() (*ServiceState, error) {
	state := &ServiceState{}
	path := filepath.Join(outDir, strings.TrimSuffix(filepath.Base(inputPath), filepath.Ext(inputPath))+".state")
	data, err := os.ReadFile(path)
	if err == nil {
		if err := json.Unmarshal(data, state); err == nil {
			return state, nil
		}
	}
	// bootstrap
	histPath := filepath.Join(outDir, strings.TrimSuffix(filepath.Base(inputPath), filepath.Ext(inputPath))+".1m")
	if fi, err := os.Stat(histPath); err == nil && fi.Size() > 0 {
		f, _ := os.Open(histPath)
		offset := fi.Size()
		size := int64(4096)
		if size > offset {
			size = offset
		}
		f.Seek(-size, io.SeekEnd)
		buf := make([]byte, size)
		f.Read(buf)
		f.Close()
		lines := strings.Split(string(buf), "\n")
		for i := len(lines) - 1; i >= 0; i-- {
			line := strings.TrimSpace(lines[i])
			if strings.HasPrefix(line, "[") {
				parts := strings.Split(strings.Trim(line, "[]"), ",")
				if len(parts) == 7 {
					epoch, _ := strconv.ParseInt(parts[6], 10, 64)
					state.LastCompletedBarEpochMs = epoch
					break
				}
			}
		}
	}
	tickFile, err := os.Open(inputPath)
	if err != nil {
		return nil, err
	}
	defer tickFile.Close()
	scanner := bufio.NewScanner(tickFile)
	currentOffset := int64(0)
	scanner.Scan() // header
	currentOffset += int64(len(scanner.Text()) + 1)
	nextStart := state.LastCompletedBarEpochMs + 60000
	prevVol := int64(0)
	for scanner.Scan() {
		line := scanner.Text()
		tick, err := parseTick(line)
		if err != nil {
			continue
		}
		lineLen := int64(len(line) + 1)
		if tick.EpochMs >= nextStart {
			state.TickFileLastReadOffset = currentOffset
			state.LastTotalVolume = prevVol
			break
		}
		prevVol = tick.TotalVol
		currentOffset += lineLen
	}
	saveState(state)
	return state, nil
}
