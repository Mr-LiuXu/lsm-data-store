package com.lx.lsmtreedb;


import com.lx.lsmtreedb.command.Command;
import com.lx.lsmtreedb.sstable.SSTableImpl;
import com.lx.lsmtreedb.wal.WAL;
import com.lx.lsmtreedb.wal.WALImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.*;
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

    /**
     * 开始。。。
     * @throws IOException
     */
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

    /***
     * 加载数据
     * @throws IOException
     */
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
     * 删除数据
     * @param key
     * @throws IOException
     */
    public void remove(String key) throws IOException {
        try {
            lock.writeLock().lock();
            Command command = new Command(Command.OP_RM, key, "");
            memTable.put(key,command);
            wal.write(command);
        }finally {
            lock.writeLock().unlock();
        }
    }

    /***
     * 获取数据
     * @param key
     * @return
     * @throws IOException
     */
    public Optional<String> get(String key) throws IOException {
        try {
            lock.readLock().lock();
            Command command = null;
            //memTable > immutableMemTable > SSTable
            if (memTable.containsKey(key)){
                command = memTable.get(key);
            }else if (immutableMemTable.containsKey(key)){
                command = immutableMemTable.get(key);
            }else {
                //查找SSTable
                command = ssTable.get(key);
            }
            if (command == null || Command.OP_RM.equals(command.getOp())){
                return Optional.empty();
            }
            if (Command.OP_PUT.equals(command.getOp())){
                return Optional.of(command.getValue());
            } else if (Command.OP_RM.equals(command.getOp())) {
                return Optional.empty();
            }else {
                throw new IllegalArgumentException("命令异常");
            }
        }finally {
            lock.readLock().unlock();
        }
    }
    /***
     * 区间查找
     * @param left
     * @param right
     * @return
     * @throws IOException
     */
    public Collection<Pair<String,String>> scan(String left,String right) throws IOException {
        TreeMap<String,Command> map = new TreeMap<>();
        for (Command command : memTable.subMap(left, true, right, true).values()) {
            map.put(command.getKey(), command);
        }
        for (Command command : immutableMemTable.subMap(left, true, right, true).values()) {
            if (!map.containsKey(command.getKey())) {
                map.put(command.getKey(), command);
            }
        }
        for (Command command : ssTable.scan(left, right)) {
            if (!map.containsKey(command.getKey())){
                map.put(command.getKey(), command);
            }
        }
        List<Pair<String,String>> list = new ArrayList<>(map.size());
        for (Command command : map.values()) {
            if (command==null||Command.OP_RM.equals(command.getOp())){
                continue;
            }
            if (Command.OP_PUT.equals(command.getOp())){
                list.add(Pair.of(command.getKey(), command.getValue()));
            }else {
                throw new IllegalArgumentException("命令异常");
            }
        }
        return list;
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

    /***
     * 停止...
     */
    public void stop() {
        lock.writeLock().lock();
        this.running = false;
        memTable.clear();
        ssTable.destory();
        lock.writeLock().unlock();
        while (persistFlag.get()){
            log.error("persist to ssTable...wait");
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        }
        immutableMemTable.clear();
    }
}
