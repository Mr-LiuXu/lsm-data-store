package com.lx.lsmtreedb.wal;

import com.lx.lsmtreedb.command.Command;

import java.io.IOException;
import java.util.Optional;

public interface WAL {
    void write(Command command) throws IOException;

    Optional<Command> read() throws IOException;

    void readSeek(long pos) throws IOException;

    void clear() throws IOException;
}
