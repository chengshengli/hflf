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

// Tick represents a parsed tick line.
type Tick struct {
	EpochMs  int64
	Price    float64
	TotalVol int64
}

// KLine represents a 1-minute OHLCV bar.
type KLine struct {
	StartTimeMs int64
	Open        float64
	High        float64
	Low         float64
	Close       float64
	Volume      int64
}

// ServiceState persists processing progress.
type ServiceState struct {
	TickFileLastReadOffset  int64 `json:"TickFileLastReadOffset"`
	LastCompletedBarEpochMs int64 `json:"LastCompletedBarEpochMs"`
	LastTotalVolume         int64 `json:"LastTotalVolume"`

	currentBar *KLine
}

// parseTick parses a tick line wrapped in 【】.
func parseTick(line string) (*Tick, error) {
	line = strings.TrimSpace(line)
	line = strings.TrimPrefix(line, "【")
	line = strings.TrimSuffix(line, "】")
	fields := strings.Split(line, ",")
	if len(fields) < 7 {
		return nil, fmt.Errorf("invalid tick line: %s", line)
	}
	price, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return nil, fmt.Errorf("parse price: %w", err)
	}
	totalVol, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse volume: %w", err)
	}
	epoch, err := parseEpochMs(fields[4])
	if err != nil {
		return nil, err
	}
	return &Tick{EpochMs: epoch, Price: price, TotalVol: totalVol}, nil
}

func parseEpochMs(t string) (int64, error) {
	tt, err := time.Parse(time.RFC3339, t)
	if err != nil {
		return 0, err
	}
	return tt.UnixMilli(), nil
}

// readLastKLineEpoch reads the last line of .1m file to get the last completed bar epoch.
func readLastKLineEpoch(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return 0, err
	}
	size := stat.Size()
	bufSize := int64(4096)
	if size < bufSize {
		bufSize = size
	}
	off := size - bufSize
	if off < 0 {
		off = 0
	}
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return 0, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return 0, err
	}
	idx := strings.LastIndexByte(string(data), '\n')
	if idx == -1 {
		if len(data) == 0 {
			return 0, nil
		}
		line := string(data)
		return parseEpochFromLine(line)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) == 0 {
		return 0, nil
	}
	return parseEpochFromLine(lines[len(lines)-1])
}

func parseEpochFromLine(line string) (int64, error) {
	line = strings.TrimSpace(line)
	line = strings.TrimPrefix(line, "【")
	line = strings.TrimSuffix(line, "】")
	fields := strings.Split(line, ",")
	if len(fields) < 7 {
		return 0, fmt.Errorf("invalid kline line: %s", line)
	}
	epoch, err := strconv.ParseInt(fields[6], 10, 64)
	if err != nil {
		return 0, err
	}
	return epoch, nil
}

// bootstrapState rebuilds ServiceState from .1m and .tick.
func bootstrapState(tickPath, logPath string) (*ServiceState, error) {
	lastEpoch, err := readLastKLineEpoch(logPath)
	if err != nil {
		return nil, err
	}
	nextEpoch := lastEpoch
	if nextEpoch != 0 {
		nextEpoch += 60000
	}

	f, err := os.Open(tickPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var offset int64
	var prevVol int64
	reader := bufio.NewReader(f)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	offset += int64(len(line)) // skip header
	for {
		startPos := offset
		line, err = reader.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		offset += int64(len(line))
		tick, err := parseTick(string(line))
		if err != nil {
			return nil, err
		}
		if tick.EpochMs >= nextEpoch {
			offset = startPos
			break
		}
		prevVol = tick.TotalVol
	}

	return &ServiceState{
		TickFileLastReadOffset:  offset,
		LastCompletedBarEpochMs: lastEpoch,
		LastTotalVolume:         prevVol,
	}, nil
}

func loadState(path string) (*ServiceState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s ServiceState
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

func saveState(path string, s *ServiceState) error {
	tmp := path + ".tmp"
	data, err := json.MarshalIndent(struct {
		TickFileLastReadOffset  int64 `json:"TickFileLastReadOffset"`
		LastCompletedBarEpochMs int64 `json:"LastCompletedBarEpochMs"`
		LastTotalVolume         int64 `json:"LastTotalVolume"`
	}{s.TickFileLastReadOffset, s.LastCompletedBarEpochMs, s.LastTotalVolume}, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func formatKLine(line *KLine, loc *time.Location) string {
	t := time.UnixMilli(line.StartTimeMs).In(loc)
	ts := t.Format("2006-01-02T15:04:05")
	return fmt.Sprintf("【%s,%.6f,%.6f,%.6f,%.6f,%d,%d】\n",
		ts, line.High, line.Low, line.Open, line.Close, line.Volume, line.StartTimeMs)
}

func finalizeCurrentBar(state *ServiceState, logPath string, loc *time.Location) error {
	if state.currentBar == nil {
		return nil
	}
	line := formatKLine(state.currentBar, loc)
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		if _, err := f.WriteString("【EasternTime,High,Low,Open,Close,Volume,Epoch】\n"); err != nil {
			return err
		}
	}
	if _, err := f.WriteString(line); err != nil {
		return err
	}
	state.LastCompletedBarEpochMs = state.currentBar.StartTimeMs
	state.currentBar = nil
	return nil
}

func flushCurrentBar(bar *KLine, path string, loc *time.Location) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.WriteString("【EasternTime,High,Low,Open,Close,Volume,Epoch】\n"); err != nil {
		return err
	}
	if bar != nil {
		if _, err := f.WriteString(formatKLine(bar, loc)); err != nil {
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func processSingleTick(state *ServiceState, tick *Tick, paths outputPaths, loc *time.Location) error {
	volumeDelta := tick.TotalVol - state.LastTotalVolume
	if volumeDelta < 0 {
		volumeDelta = tick.TotalVol
	}
	state.LastTotalVolume = tick.TotalVol

	minuteEpoch := tick.EpochMs - (tick.EpochMs % 60000)
	if state.currentBar == nil || minuteEpoch > state.currentBar.StartTimeMs {
		if state.currentBar != nil {
			if err := finalizeCurrentBar(state, paths.logPath, loc); err != nil {
				return err
			}
		}
		state.currentBar = &KLine{
			StartTimeMs: minuteEpoch,
			Open:        tick.Price,
			High:        tick.Price,
			Low:         tick.Price,
			Close:       tick.Price,
			Volume:      volumeDelta,
		}
	} else {
		if tick.Price > state.currentBar.High {
			state.currentBar.High = tick.Price
		}
		if tick.Price < state.currentBar.Low {
			state.currentBar.Low = tick.Price
		}
		state.currentBar.Close = tick.Price
		state.currentBar.Volume += volumeDelta
	}
	return nil
}

type outputPaths struct {
	logPath     string
	currentPath string
	statePath   string
}

func processNewTicks(state *ServiceState, tickPath string, paths outputPaths, loc *time.Location, mu *sync.Mutex, debug bool) error {
	mu.Lock()
	defer mu.Unlock()

	f, err := os.Open(tickPath)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Seek(state.TickFileLastReadOffset, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReader(f)
	if state.TickFileLastReadOffset == 0 {
		line, err := r.ReadBytes('\n')
		if err != nil {
			return err
		}
		state.TickFileLastReadOffset += int64(len(line))
	}
	for {
		line, err := r.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		state.TickFileLastReadOffset += int64(len(line))
		tick, err := parseTick(string(line))
		if err != nil {
			return err
		}
		if debug {
			fmt.Printf("tick: %+v\n", tick)
		}
		if err := processSingleTick(state, tick, paths, loc); err != nil {
			return err
		}
	}
	if err := flushCurrentBar(state.currentBar, paths.currentPath, loc); err != nil {
		return err
	}
	if err := saveState(paths.statePath, state); err != nil {
		return err
	}
	return nil
}

func loadOrBootstrap(statePath, tickPath, logPath string) (*ServiceState, error) {
	s, err := loadState(statePath)
	if err == nil {
		return s, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		fmt.Printf("state load error: %v, rebuilding...\n", err)
	}
	return bootstrapState(tickPath, logPath)
}

func watchTickFile(ctx context.Context, path string, debounce time.Duration, cb func()) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := watcher.Add(dir); err != nil {
		return err
	}
	go func() {
		defer watcher.Close()
		var timer *time.Timer
		for {
			select {
			case <-ctx.Done():
				if timer != nil {
					timer.Stop()
				}
				return
			case ev := <-watcher.Events:
				if ev.Name == path && ev.Op&(fsnotify.Write|fsnotify.Create) != 0 {
					if timer != nil {
						timer.Stop()
					}
					timer = time.AfterFunc(debounce, cb)
				}
			case err := <-watcher.Errors:
				fmt.Fprintf(os.Stderr, "fsnotify error: %v\n", err)
			}
		}
	}()
	return nil
}

func main() {
	inputPath := flag.String("input", "", "path to .tick file")
	outDir := flag.String("out-dir", "", "output directory")
	debug := flag.Bool("debug-print", false, "enable debug logs")
	flag.Parse()

	if *inputPath == "" || *outDir == "" {
		fmt.Fprintln(os.Stderr, "-input and -out-dir required")
		os.Exit(1)
	}
	if _, err := os.Stat(*inputPath); err != nil {
		fmt.Fprintf(os.Stderr, "input file error: %v\n", err)
		os.Exit(1)
	}
	if err := os.MkdirAll(*outDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir out-dir: %v\n", err)
		os.Exit(1)
	}

	base := strings.TrimSuffix(filepath.Base(*inputPath), filepath.Ext(*inputPath))
	paths := outputPaths{
		logPath:     filepath.Join(*outDir, base+".1m"),
		currentPath: filepath.Join(*outDir, base+".1m.current"),
		statePath:   filepath.Join(*outDir, base+".state"),
	}

	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		fmt.Fprintf(os.Stderr, "load tz: %v\n", err)
		os.Exit(1)
	}

	state, err := loadOrBootstrap(paths.statePath, *inputPath, paths.logPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bootstrap state: %v\n", err)
		os.Exit(1)
	}

	var mu sync.Mutex
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := watchTickFile(ctx, *inputPath, 500*time.Millisecond, func() {
		if err := processNewTicks(state, *inputPath, paths, loc, &mu, *debug); err != nil {
			fmt.Fprintf(os.Stderr, "process ticks: %v\n", err)
		}
	}); err != nil {
		fmt.Fprintf(os.Stderr, "watch error: %v\n", err)
		os.Exit(1)
	}

	// Initial run to process any existing ticks
	if err := processNewTicks(state, *inputPath, paths, loc, &mu, *debug); err != nil {
		fmt.Fprintf(os.Stderr, "initial processing error: %v\n", err)
	}

	<-ctx.Done()

	mu.Lock()
	if state.currentBar != nil {
		if err := finalizeCurrentBar(state, paths.logPath, loc); err != nil {
			fmt.Fprintf(os.Stderr, "finalize on shutdown: %v\n", err)
		}
	}
	if err := flushCurrentBar(state.currentBar, paths.currentPath, loc); err != nil {
		fmt.Fprintf(os.Stderr, "flush on shutdown: %v\n", err)
	}
	if err := saveState(paths.statePath, state); err != nil {
		fmt.Fprintf(os.Stderr, "save state on shutdown: %v\n", err)
	}
	mu.Unlock()
}
