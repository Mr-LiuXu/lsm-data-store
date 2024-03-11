package com.lx.lsmtreedb.segment;

import com.alibaba.fastjson.JSON;
import com.lx.lsmtreedb.command.Command;
import com.lx.lsmtreedb.sstable.SparseIndex;
import com.lx.lsmtreedb.utils.FileUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collection;
import java.util.TreeMap;

@Slf4j
public class SegmentImpl implements Segment{
    private String path;
    private int segmentId;
    private int partSize;
    private SparseIndex sparseIndex;
    private RandomAccessFile reader;
    public SegmentImpl(String path, int segmentId, int partSize) {
        this.path = path;
        this.segmentId = segmentId;
        this.partSize = partSize;
    }

    @Override
    public void reload() throws IOException {

    }

    @Override
    public Command get(String key) throws IOException {
        return null;
    }

    @Override
    public Collection<Command> scan(String left, String right) throws IOException {
        return null;
    }

    @Override
    public void persist(TreeMap<String, Command> memTable) throws IOException {
        log.info("persist[start]...segmentId:{},memTable size:{}", segmentId, memTable.size());
        String filename = FileUtil.buildFilename(path, String.valueOf(segmentId));
        RandomAccessFile writer = new RandomAccessFile(filename, "rw");
        this.reader = new RandomAccessFile(filename, "r");
        writer.seek(SegmentMetaData.META_DATA_SIZE);
        long offset = SegmentMetaData.META_DATA_SIZE;
        int size = 0;
        int dataSize = 0;
        this.sparseIndex = new SparseIndex();
        String sparseIndexKey = "";
        for (Command command : memTable.values()) {
            if (StringUtils.isBlank(sparseIndexKey)){
                sparseIndexKey = command.getKey();
            }
            byte[] json = JSON.toJSONBytes(command);
            writer.writeInt(json.length);
            writer.write(json);
            int len = 4+ json.length;
            size += len;
            dataSize +=len;
            if (size >= partSize){
                //write sparse index
                sparseIndex.addIndex(sparseIndexKey,offset,size);
                offset += size;
                size = 0;
                sparseIndexKey = "";
            }
        }
        if (size>0){
            //写入稀疏索引
            sparseIndex.addIndex(sparseIndexKey,offset,size);
        }
        //稀疏索引持久化
        byte[] indexData = sparseIndex.toByteArray();
        writer.write(indexData);
        //写入元信息
        SegmentMetaData metaData = new SegmentMetaData(SegmentMetaData.META_DATA_SIZE, dataSize, SegmentMetaData.META_DATA_SIZE, indexData.length);
        writer.seek(0);
        writer.write(metaData.toByteArray());
        writer.close();
        log.info("persist[success]...segmentId:{},memTable size:{}", segmentId, memTable.size());
    }
}
