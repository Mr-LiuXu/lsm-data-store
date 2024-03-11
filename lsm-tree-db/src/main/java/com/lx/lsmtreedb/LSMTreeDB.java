package com.lx.lsmtreedb;


import com.lx.lsmtreedb.command.Command;
import com.lx.lsmtreedb.sstable.SSTableImpl;
import com.lx.lsmtreedb.wal.WAL;
import com.lx.lsmtreedb.wal.WALImpl;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;
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
        final LSMTreeDB db = this;
        Thread checkPersist = new Thread(()->{
            while (db.running){
              try {
                  db.memTablePersist();
                  Thread.sleep(1000);
              } catch (IOException e) {
                  throw new RuntimeException(e);
              } catch (InterruptedException e) {
                  throw new RuntimeException(e);
              }
            }
        });
        checkPersist.start();
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

    /***
     * 写入数据
     * @param key
     * @param value
     * @throws IOException
     */
    public void put(String key,String value) throws IOException {
        try {
            lock.writeLock().lock();
            Command command = new Command(Command.OP_PUT, key, value);
            memTable.put(key,command);
            wal.write(command);
        }finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Determine whether to execute persist
     */
    public void memTablePersist() throws IOException {
        if (memTable.size() <= MEM_TABLE_MAX_SIZE || persistFlag.get()) {
            return;
        }
        doMemTablePersist();
    }
    public void doMemTablePersist() throws IOException {
        if (persistFlag.compareAndExchange(false,true)){
            return;
        }
        log.info("memTable persist[start]...");
        lock.writeLock().lock();
        for (Map.Entry<String, Command> entry : memTable.entrySet()) {
            String key = entry.getKey();
            Command command = entry.getValue();
            immutableMemTable.put(key,command);
        }
        memTable.clear();
        lock.writeLock().unlock();
        ssTable.persistent(immutableMemTable);
        lock.writeLock().lock();
        immutableMemTable.clear();
        //clear the wal
        wal.clear();
        lock.writeLock().unlock();
        persistFlag.compareAndExchange(true,true);
        log.info("memTable persist[finish]...");
    }
}
