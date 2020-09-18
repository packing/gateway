package main

import (
    "sync"

    "github.com/packing/nbpy/nnet"
)

type AdapterInfo struct {
    pid int
    host string
    connection int
    unixAddr string
    unixMsgAddr string
}

var (
    GlobalAdapters = make(map[nnet.SessionID] AdapterInfo)
    adapterLock sync.Mutex
)

func addAdapter(adapterId nnet.SessionID, ai AdapterInfo) {
    adapterLock.Lock()
    defer adapterLock.Unlock()
    GlobalAdapters[adapterId] = ai
}

func delAdapter(adapterId nnet.SessionID) {
    adapterLock.Lock()
    defer adapterLock.Unlock()
    delete(GlobalAdapters, adapterId)
}

func pollFreeAdapter() *AdapterInfo {
    adapterLock.Lock()
    defer adapterLock.Unlock()
    var poll *AdapterInfo = nil
    for _, si := range GlobalAdapters {
        if si.host != localhost {
            continue
        }
        if poll == nil {
            poll = &si
            continue
        }
        if si.connection == 0 || si.connection < poll.connection {
            poll = &si
        }
    }
    return poll
}