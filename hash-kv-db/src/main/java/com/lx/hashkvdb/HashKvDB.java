package com.lx.hashkvdb;

import com.alibaba.fastjson.JSON;
import com.lx.hashkvdb.command.Command;
import com.lx.hashkvdb.command.CommandPos;
import com.lx.hashkvdb.command.CommandType;
import com.lx.hashkvdb.utils.DBUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
@Slf4j
public class HashKvDB {
    private Map<Integer, RandomAccessFile> readerMap; //记录写入的文件地址的
    private RandomAccessFile writer; //写入地址
    private Map<String, CommandPos> index; //索引
    private String path;
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    //日志压缩相关
    private int curLogId;
    //已经压缩的日志，一般是curLogId-1
    private int compactLogId;
    //用于记录curLogId和compactLogId
    private RandomAccessFile logIndexRW;
    private volatile boolean running;
    public HashKvDB(String path) {
        index = new HashMap<>();
        readerMap = new HashMap<>();
        this.path = path;
        this.running = false;
    }

    /**
     *启动任务
     */
    public void  start(){
        running = true;
        final HashKvDB db = this;
        Thread checkCompact = new Thread(()->{
            while (db.running){
                try {
                    db.compact();
                    Thread.sleep(1000);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        checkCompact.start();
    }
    /***
     * 写入数据
     * @param key
     * @param value
     * @return
     * @throws IOException
     */
    public boolean put(String key,String value) throws IOException {
        try {
            lock.writeLock().lock();
            long pos = writer.length();
            Command cmd = new Command(CommandType.OP_PUT, key, value);
            byte[] json = JSON.toJSONBytes(cmd);
            //顺序写,4个byte是长度，剩下是内容
            writer.writeInt(json.length);
            writer.write(json);
            index.put(key,new CommandPos(pos,curLogId));
            return true;
        }finally {
            lock.writeLock().unlock();
        }
    }
    /***
     * 判断是否启动合并日志
     * @throws IOException
     */
     public void  compact() throws IOException {
        if (writer.length() <= CommandType.LOG_MX_SIZE){
            return;
        }
         compactLog();
     }

    /***
     * 日志文件压缩 write on cope
     * @throws IOException
     */
     public void compactLog() throws IOException {
        lock.writeLock().lock(); //写锁
        int compactTo = curLogId + 1; //需要合并的文件
        int compactFrom = curLogId; //旧文件
        curLogId = compactTo + 1; //新写入的文件
        log.info("compact[start]...compactTo:{}", compactTo);
         Map<String, CommandPos> dumpIndex = dump();//实现WOC
         RandomAccessFile compactWriter = new RandomAccessFile(DBUtils.buildFilename(path, String.valueOf(curLogId)), "rw");
         readerMap.put(curLogId,new RandomAccessFile(DBUtils.buildFilename(path,String.valueOf(curLogId)),"r"));
         readerMap.put(compactTo,new RandomAccessFile(DBUtils.buildFilename(path,String.valueOf(compactTo)),"r"));
         lock.writeLock().unlock();
         //进行日志合并，因为使用了Copy-On-Write，所以不需要加锁
         long pos=0;
         byte[] buffer = new byte[CommandType.BUFFER_MX_SIZE];
         HashMap<String, CommandPos> indexUpdate = new HashMap<>();
         for (Map.Entry<String, CommandPos> posEntry : dumpIndex.entrySet()) {
             String key = posEntry.getKey();
             CommandPos commandPos = posEntry.getValue();
             if (commandPos.getLogId() == curLogId){
                 continue; //写入到新文件的key不需要压缩
             }
             RandomAccessFile reader = readerMap.get(commandPos.getLogId());
             reader.seek(commandPos.getPos());
             int len = reader.readInt();
             reader.read(buffer,0,len);
             compactWriter.writeInt(len);
             compactWriter.write(buffer,0,len);
             indexUpdate.put(key,new CommandPos(pos,compactTo));
             pos += 4 + len;
         }
         //更新索引和对应的日志文件
         lock.writeLock().lock();
         for (Map.Entry<String, CommandPos> posEntry : indexUpdate.entrySet()) {
             String key = posEntry.getKey();
             CommandPos commandPos = posEntry.getValue();
             //后面有更新的场景
             if (!index.containsKey(key)||index.get(key).getLogId() == curLogId){
                 continue;
             }
             index.put(key,commandPos);
         }
         //删除压缩前的文件
         HashSet<Integer> delSet = new HashSet<>();
         for (Integer logId : readerMap.keySet()) {
             if (logId <= compactFrom){
                 delSet.add(logId);
             }
         }
         for (Integer logId : delSet) {
             readerMap.get(logId).close();
             readerMap.remove(logId);
             File file = new File(DBUtils.buildFilename(path, String.valueOf(logId)));
             file.delete();
         }
         compactLogId = compactTo;
         logIndexRW.seek(0);
         logIndexRW.writeInt(curLogId);
         logIndexRW.writeInt(compactLogId);
         log.info("compact[finish]...compactLogId:{},curLogId:{}", compactLogId, curLogId);
         lock.writeLock().unlock();
     }

    /***
     * 将索引复制出来一份
     * @return
     */
    private Map<String,CommandPos> dump(){
         HashMap<String, CommandPos> posHashMap = new HashMap<>();
         for (Map.Entry<String, CommandPos> posEntry : index.entrySet()) {
             CommandPos commandPos = posEntry.getValue();
             posHashMap.put(posEntry.getKey(),new CommandPos(commandPos.getPos(),commandPos.getLogId()));
         }
         return posHashMap;
     }

    /***
     * 停止
     */
    public void stop(){
        lock.writeLock().lock();
        running = false;
        index.clear();
        lock.writeLock().unlock();
     }


}
