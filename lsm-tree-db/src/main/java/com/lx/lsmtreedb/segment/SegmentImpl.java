package com.lx.lsmtreedb.segment;

import com.lx.lsmtreedb.command.Command;
import com.lx.lsmtreedb.sstable.SparseIndex;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collection;
import java.util.TreeMap;

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

    }
}
