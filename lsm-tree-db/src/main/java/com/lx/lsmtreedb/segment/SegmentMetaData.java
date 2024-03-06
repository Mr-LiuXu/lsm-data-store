package com.lx.lsmtreedb.segment;

public class SegmentMetaData {
    public static final int META_DATA_SIZE = 8 + 4 + 6 + 4;
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

}
