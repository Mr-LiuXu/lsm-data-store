package com.lx.lsmtreedb.segment;

import java.nio.ByteBuffer;

public class SegmentMetaData {
    public static final int META_DATA_SIZE = 8 + 4 + 8 + 4;
    private long dataOffset;
    private int dataLen;
    private long indexOffset;
    private int indexLen;

    private SegmentMetaData(){

    }
    public SegmentMetaData(long dataOffset,int dataLen,long indexOffset,int indexLen){
        this.dataOffset = dataOffset;
        this.dataLen = dataLen;
        this.indexOffset = indexOffset;
        this.indexLen = indexLen;
    }

    public static SegmentMetaData readBytes(byte[] arr){
        SegmentMetaData metaData = new SegmentMetaData();
        ByteBuffer buffer = ByteBuffer.wrap(arr);
        int offset = 0;
        metaData.dataOffset = buffer.getLong(offset);
        offset += 8;
        metaData.dataLen = buffer.getInt(offset);
        offset +=4;
        metaData.indexOffset = buffer.getLong(offset);
        offset += 8;
        metaData.indexLen = buffer.getInt(offset);
        return metaData;
    }

    public byte[] toByteArray(){
        ByteBuffer buffer = ByteBuffer.allocate(META_DATA_SIZE);
        buffer.putLong(dataOffset);
        buffer.putInt(dataLen);
        buffer.putLong(indexOffset);
        buffer.putInt(indexLen);
        return buffer.array();
    }

    public long getDataOffset() {
        return dataOffset;
    }

    public void setDataOffset(long dataOffset) {
        this.dataOffset = dataOffset;
    }

    public int getDataLen() {
        return dataLen;
    }

    public void setDataLen(int dataLen) {
        this.dataLen = dataLen;
    }

    public long getIndexOffset() {
        return indexOffset;
    }

    public void setIndexOffset(long indexOffset) {
        this.indexOffset = indexOffset;
    }

    public int getIndexLen() {
        return indexLen;
    }

    public void setIndexLen(int indexLen) {
        this.indexLen = indexLen;
    }

    @Override
    public String toString() {
        return "SegmentMetaData{" +
                "dataOffset=" + dataOffset +
                ", dataLen=" + dataLen +
                ", indexOffset=" + indexOffset +
                ", indexLen=" + indexLen +
                '}';
    }
}
