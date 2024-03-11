package com.lx.lsmtreedb.sstable;

public class SparseIndexItem {
    private String key;
    private long offset;
    private int len;

    public SparseIndexItem(){

    }

    public SparseIndexItem(String key, long offset, int len) {
        this.key = key;
        this.offset = offset;
        this.len = len;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public int getLen() {
        return len;
    }

    public void setLen(int len) {
        this.len = len;
    }
}
