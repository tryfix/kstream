package join

import "sync"

type Window struct {
	l      *sync.Mutex
	window map[interface{}]interface{}
}

func NewWindow() *Window {
	return &Window{
		l:      new(sync.Mutex),
		window: make(map[interface{}]interface{}),
	}
}

func (w *Window) Write(key, value interface{}) {
	w.l.Lock()
	defer w.l.Unlock()
	w.window[key] = value
}

func (w *Window) Read(key interface{}) (interface{}, bool) {
	w.l.Lock()
	defer w.l.Unlock()

	v, ok := w.window[key]
	return v, ok
}
