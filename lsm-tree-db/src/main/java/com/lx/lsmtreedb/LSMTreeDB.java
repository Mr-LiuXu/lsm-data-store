package com.lx.lsmtreedb;


import com.lx.lsmtreedb.command.Command;
import com.lx.lsmtreedb.sstable.SSTableImpl;
import com.lx.lsmtreedb.wal.WAL;
import com.lx.lsmtreedb.wal.WALImpl;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class LSMTreeDB {
    public static final int PART_SIZE=1024;
    public static final int MEM_TABLE_MAX_SIZE = 10;
    private TreeMap<String, Command> memTable;
    private TreeMap<String,Command> immutableMemTable;
    private SSTableImpl ssTable;
    private volatile boolean running;
    private WAL wal;
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    AtomicBoolean persistFlag;

    public LSMTreeDB(String path) throws IOException {
        this.memTable = new TreeMap<>();
        this.immutableMemTable = new TreeMap<>();
        this.ssTable = new SSTableImpl(0,PART_SIZE,path);
        this.running = false;
        this.wal = new WALImpl(path);
        this.persistFlag = new AtomicBoolean(false);
    }

    public void start() throws IOException {
        lock.writeLock().lock();
        this.running = true;
        reload();
        lock.writeLock().unlock();
    }

    public void  reload() throws IOException {
        wal.readSeek(0);
        while (true){
            Optional<Command> opt = wal.read();
            if (opt.isEmpty()) {
                break;
            }
            Command command = opt.get();
            this.memTable.put(command.getKey(), command);
        }
        ssTable.reload();
    }
}
