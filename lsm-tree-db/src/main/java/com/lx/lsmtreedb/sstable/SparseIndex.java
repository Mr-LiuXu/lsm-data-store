package com.lx.lsmtreedb.sstable;

import com.alibaba.fastjson.JSON;

import java.util.ArrayList;
import java.util.List;

public class SparseIndex {
    public static final int DEF_SIZE = 1024;
    private List<SparseIndexItem> indexItems;

    public SparseIndex() {
        this.indexItems = new ArrayList<>(DEF_SIZE);
    }

    public void addIndex(String key,long offset, int len){
        indexItems.add(new SparseIndexItem(key,offset,len));
    }

    public byte[] toByteArray(){
        return JSON.toJSONBytes(indexItems);
    }

    public static SparseIndex parse(byte[] buffer){
        List<SparseIndexItem> list = JSON.parseArray(new String(buffer), SparseIndexItem.class);
        SparseIndex sparseIndex = new SparseIndex();
        sparseIndex.indexItems.addAll(list);
        return sparseIndex;
    }

    public SparseIndexItem findFirst(String key){
        if (indexItems.size()==0){
            return null;
        }
        if (indexItems.get(0).getKey().compareTo(key) > 0){
            return null;
        }
        int l = 0,r = indexItems.size() -1;
        while (l < r){
            int mid = l + (r - l + 1) /2;
            //TODO
        }
        return indexItems.get(l);
    }

    public List<SparseIndexItem> getIndexItems() {
        return indexItems;
    }

    public void setIndexItems(List<SparseIndexItem> indexItems) {
        this.indexItems = indexItems;
    }
}
