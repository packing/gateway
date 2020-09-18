package main

import (
    "github.com/packing/nbpy/codecs"
    "github.com/packing/nbpy/messages"
    "github.com/packing/nbpy/nnet"
    "github.com/packing/nbpy/utils"
)

type GatewayMessageObject struct {
}


func OnAdapters(msg *messages.Message) (error) {
    body := msg.GetBody()
    if body == nil {
        return nil
    }
    iLocalhost, ok := body[messages.ProtocolKeyLocalHost]
    if ok {
        localhost, _ = iLocalhost.(string)
        utils.LogInfo("握手成功, 本地主机为 %s", localhost)
    }
    iList, ok := body[messages.ProtocolKeyValue]
    if ok {
        list, ok := iList.(codecs.IMSlice)
        if ok {
            for _, l := range list {
                m, ok := l.(codecs.IMMap)
                if ok {
                    mr := codecs.CreateMapReader(m)
                    iSessionId := mr.TryReadValue(messages.ProtocolKeySessionId)
                    sessionId, ok := iSessionId.(uint)
                    if ok {
                        var ai AdapterInfo
                        ai.unixMsgAddr = mr.StrValueOf(messages.ProtocolKeyUnixMsgAddr, "")
                        ai.host = mr.StrValueOf(messages.ProtocolKeyHost, "")
                        ai.unixAddr = mr.StrValueOf(messages.ProtocolKeyUnixAddr, "")
                        ai.pid = int(mr.IntValueOf(messages.ProtocolKeyId, 0))
                        ai.connection = int(mr.IntValueOf(messages.ProtocolKeyValue, 0))
                        addAdapter(nnet.SessionID(sessionId), ai)
                    }
                }
            }
        }
    }
    return nil
}


func OnAdapterCome(msg *messages.Message) (error) {
    body := msg.GetBody()
    if body == nil {
        return nil
    }

    mr := codecs.CreateMapReader(body)
    iSessionId := mr.TryReadValue(messages.ProtocolKeySessionId)
    sessionId, ok := iSessionId.(uint)
    if ok {
        var si AdapterInfo
        si.unixMsgAddr = mr.StrValueOf(messages.ProtocolKeyUnixMsgAddr, "")
        si.host = mr.StrValueOf(messages.ProtocolKeyHost, "")
        si.unixAddr = mr.StrValueOf(messages.ProtocolKeyUnixAddr, "")
        si.pid = int(mr.IntValueOf(messages.ProtocolKeyId, 0))
        si.connection = int(mr.IntValueOf(messages.ProtocolKeyValue, 0))
        addAdapter(nnet.SessionID(sessionId), si)

        utils.LogInfo("Adapter %s - %d (%d) 上线", si.host, si.pid, sessionId)
    }

    return nil
}

func OnAdapterBye(msg *messages.Message) (error) {
    body := msg.GetBody()
    if body == nil {
        return nil
    }

    mr := codecs.CreateMapReader(body)
    iSessionId := mr.TryReadValue(messages.ProtocolKeySessionId)
    sessionId, ok := iSessionId.(uint)
    if ok {
        delAdapter(nnet.SessionID(sessionId))
        utils.LogInfo("Adapter (%d) 离线", sessionId)
    }

    return nil
}

func OnAdapterChange(msg *messages.Message) (error) {
    body := msg.GetBody()
    if body == nil {
        return nil
    }

    mr := codecs.CreateMapReader(body)
    iSessionId := mr.TryReadValue(messages.ProtocolKeySessionId)
    sessionId, ok := iSessionId.(uint)
    if ok {
        var si AdapterInfo
        si.unixMsgAddr = mr.StrValueOf(messages.ProtocolKeyUnixMsgAddr, "")
        si.host = mr.StrValueOf(messages.ProtocolKeyHost, "")
        si.unixAddr = mr.StrValueOf(messages.ProtocolKeyUnixAddr, "")
        si.pid = int(mr.IntValueOf(messages.ProtocolKeyId, 0))
        si.connection = int(mr.IntValueOf(messages.ProtocolKeyValue, 0))
        addAdapter(nnet.SessionID(sessionId), si)
        //utils.LogInfo("Adapter %s - %d (%d) 状态更新", si.host, si.pid, sessionId)
    }

    return nil
}

func (receiver GatewayMessageObject) GetMappedTypes() (map[int]messages.MessageProcFunc) {
    msgMap := make(map[int]messages.MessageProcFunc)

    msgMap[messages.ProtocolTypeAdapters] = OnAdapters
    msgMap[messages.ProtocolTypeAdapterCome] = OnAdapterCome
    msgMap[messages.ProtocolTypeAdapterBye] = OnAdapterBye
    msgMap[messages.ProtocolTypeAdapterChange] = OnAdapterChange

    return msgMap
}