package com.lx.hashkvdb.command;

public class CommandPos {
    long pos;
    //长度直接在文件里面标识
    //int len;
    //对应的日志文件
    int logId;

    public CommandPos(long pos, int logId) {
        this.pos = pos;
        //this.len = len;
        this.logId = logId;
    }

    public long getPos() {
        return pos;
    }

    public void setPos(long pos) {
        this.pos = pos;
    }

    public int getLogId() {
        return logId;
    }

    public void setLogId(int logId) {
        this.logId = logId;
    }
}
