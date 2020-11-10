package main

import (
    "flag"
    "fmt"
    "log"
    "net"

    "github.com/packing/nbpy/codecs"
    "github.com/packing/nbpy/env"
    "github.com/packing/nbpy/messages"
    "github.com/packing/nbpy/nnet"
    "github.com/packing/nbpy/packets"
    "github.com/packing/nbpy/utils"

    "os"
    "runtime"
    "runtime/pprof"
    "syscall"
)

var (
    help    bool
    version bool

    daemon   bool
    setsided bool

    pprofFile  string
    addr       string
    addrMaster string
    port       int

    localhost string

    logDir   string
    logLevel = utils.LogLevelVerbose
    pidFile  string

    tcp     *nnet.TCPServer = nil
    tcpCtrl *nnet.TCPClient = nil
    unixMsg *nnet.UnixMsg   = nil
)

func usage() {
    fmt.Fprint(os.Stderr, `adapter

Usage: gateway [-hv] [-d daemon] [-c master addr] [-a addr] [-f pprof file]

Options:
`)
    flag.PrintDefaults()
}

func sayHello() error {
    defer func() {
        utils.LogPanic(recover())
    }()
    msg := messages.CreateS2SMessage(messages.ProtocolTypeGatewayHello)
    msg.SetTag(messages.ProtocolTagMaster)
    req := codecs.IMMap{}
    req[messages.ProtocolKeyId] = os.Getpid()
    req[messages.ProtocolKeyValue] = addr
    msg.SetBody(req)
    pck, err := messages.DataFromMessage(msg)
    if err == nil {
        tcpCtrl.Send(pck)
    }
    return err
}

func OnConnectAccepted(conn net.Conn) error {
    ai := pollFreeAdapter()
    if ai != nil {
        if s, ok := conn.(*net.TCPConn); ok {
            if pf, err := s.File(); err == nil {
                utils.LogError(">>> 转移新连接句柄 %d => %s", int(pf.Fd()), ai.unixMsgAddr)
                unixMsg.SendTo(ai.unixMsgAddr, int(pf.Fd()))
            }
        }
    }

    return nil
}

func main() {

    flag.BoolVar(&help, "h", false, "this help")
    flag.BoolVar(&version, "v", false, "print version")
    flag.BoolVar(&daemon, "d", false, "run at daemon")
    flag.BoolVar(&setsided, "s", false, "already run at daemon")
    flag.StringVar(&addr, "a", "0.0.0.0:10086", "listen addr")
    flag.StringVar(&addrMaster, "c", "127.0.0.1:10088", "controller addr")
    flag.StringVar(&pprofFile, "f", "", "pprof file")
    flag.Usage = usage

    flag.Parse()
    if help {
        flag.Usage()
        return
    }
    if version {
        fmt.Println("adapter version 1.0")
        return
    }

    logDir = "./logs/gateway"
    if !daemon {
        logDir = ""
    } else {
        if !setsided {
            utils.Daemon()
            return
        }
    }

    pidFile = "./pid"
    utils.GeneratePID(pidFile)

    unixMsgAddr := fmt.Sprintf("/tmp/gateway_msg_%d.sock", os.Getpid())

    var pproff *os.File = nil
    if pprofFile != "" {
        pf, err := os.OpenFile(pprofFile, os.O_RDWR|os.O_CREATE, 0644)
        if err != nil {
            log.Fatal(err)
        }
        pproff = pf
        pprof.StartCPUProfile(pproff)
    }

    defer func() {
        if pproff != nil {
            pprof.StopCPUProfile()
            pproff.Close()
        }

        utils.RemovePID(pidFile)

        syscall.Unlink(unixMsgAddr)

        utils.LogInfo(">>> 进程已退出")
    }()

    utils.LogInit(logLevel, logDir)
    //注册解码器
    env.RegisterCodec(codecs.CodecIMv2)
    env.RegisterCodec(codecs.CodecJSONv1)

    //注册通信协议
    env.RegisterPacketFormat(packets.PacketFormatNB)
    env.RegisterPacketFormat(packets.PacketFormatHTTP)
    env.RegisterPacketFormat(packets.PacketFormatWS)

    messages.GlobalDispatcher.MessageObjectMapped(messages.ProtocolSchemeS2S, messages.ProtocolTagClient, GatewayMessageObject{})
    messages.GlobalDispatcher.Dispatch()

    unixMsg = nnet.CreateUnixMsg()
    err := unixMsg.Bind(unixMsgAddr)
    if err != nil {
        utils.LogError("!!!无法创建unix句柄管道 %s", unixMsgAddr, err)
        unixMsg.Close()
        return
    }

    tcp = nnet.CreateTCPServer()
    tcp.OnConnectAccepted = OnConnectAccepted
    err = tcp.Bind(addr, port)
    if err != nil {
        utils.LogError("!!!无法绑定tcp地址 %s:%d", addr, port, err)
        unixMsg.Close()
        tcp.Close()
        return
    }
    tcp.Schedule()

    tcpCtrl = nnet.CreateTCPClient(packets.PacketFormatNB, codecs.CodecIMv2)
    tcpCtrl.OnDataDecoded = messages.GlobalMessageQueue.Push
    err = tcpCtrl.Connect(addrMaster, 0)
    if err != nil {
        utils.LogError("!!!无法连接到控制服务器 %s", addr, err)
        unixMsg.Close()
        tcp.Close()
        tcpCtrl.Close()
        return
    } else {
        sayHello()
    }

    utils.LogInfo(">>> 当前协程数量 > %d", runtime.NumGoroutine())
    //开启调度，主线程停留在此等候信号
    env.Schedule()

    tcpCtrl.Close()
    tcp.Close()
    unixMsg.Close()

}
