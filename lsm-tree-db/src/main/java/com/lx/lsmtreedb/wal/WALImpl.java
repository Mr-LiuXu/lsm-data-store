package com.lx.lsmtreedb.wal;

import com.alibaba.fastjson.JSON;
import com.lx.lsmtreedb.command.Command;
import com.lx.lsmtreedb.utils.FileUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Optional;
import java.util.RandomAccess;

public class WALImpl implements WAL{
    public static final String FILE_NAME = "wal";
    public static final String TMP_FILE_NAME = "wal.tmp";
    private String path;
    RandomAccessFile writer;
    RandomAccessFile reader;

    @Override
    public void write(Command command) throws IOException {
        byte[] json = JSON.toJSONBytes(command);
        this.writer.writeInt(json.length);
        this.writer.write(json);
    }

    @Override
    public Optional<Command> read() throws IOException {
        int size;
        try {
           size =  this.reader.readInt();
        } catch (IOException e) {
            return Optional.empty();
        }
        if (size > 0){
            byte[] buffer = new byte[size];
            this.reader.read(buffer);
            Command command = JSON.parseObject(buffer, Command.class);
            return Optional.of(command);
        }else {
            return Optional.empty();
        }
    }

    @Override
    public void readSeek(long pos) throws IOException {
        this.reader.seek(pos);
    }

    @Override
    public void clear() throws IOException {
        this.reader.close();
        this.writer.close();
        File cur = new File(FileUtil.buildFilename(path, FILE_NAME));
        File rename = new File(TMP_FILE_NAME);
        cur.renameTo(rename);
        rename.delete();
        this.writer = new RandomAccessFile(FileUtil.buildFilename(path, FILE_NAME),"rw");
        this.reader = new RandomAccessFile(FileUtil.buildFilename(path, FILE_NAME),"r");
    }
    public WALImpl(String path) throws IOException{
        this.path = path;
        this.writer = new RandomAccessFile(FileUtil.buildFilename(path, FILE_NAME), "rw");
        this.reader = new RandomAccessFile(FileUtil.buildFilename(path, FILE_NAME), "r");
        writer.seek(writer.length());
    }
}
