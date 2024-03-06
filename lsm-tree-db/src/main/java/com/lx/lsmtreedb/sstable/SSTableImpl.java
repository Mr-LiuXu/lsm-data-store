package com.lx.lsmtreedb.sstable;

import com.lx.lsmtreedb.command.Command;
import com.lx.lsmtreedb.segment.Segment;
import com.lx.lsmtreedb.segment.SegmentImpl;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class SSTableImpl {
    private int segmentId;
    private int partSize;
    private String path;
    Deque<Segment> segments;

    public SSTableImpl(int segmentId, int partSize, String path) {
        this.segmentId = segmentId;
        this.partSize = partSize;
        this.path = path;
        this.segments = new LinkedList<>();
    }
    public Command get(String key) throws IOException {
        for (Segment segment : segments) {
            Command command = segment.get(key);
            if (command!=null) {
                return command;
            }
        }
        return null;
    }
    public void reload() throws IOException {
        segments.clear();
        File dir = new File(path);
        if (dir==null || !dir.isDirectory()){
            return;
        }
        File[] files = dir.listFiles();
        List<File> fileList = Arrays.stream(files).filter(f -> !f.getName().equals("wal.log")).sorted(Comparator.comparing(File::getName))
                .collect(Collectors.toList());
        for (File file : fileList) {
            int segmentId = Integer.parseInt(file.getName().split("\\.")[0]);
            SegmentImpl segment = new SegmentImpl(path, segmentId, partSize);
            segment.reload();
            segments.offerLast(segment);
        }
    }

}
