package me.jeffreyu.collect;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

class Page {
    
    final MappedByteBuffer buffer;

    final int id;
    final File file;
    final long pageSize;
    final int idxNextPage;

    // pageSize limit to Integer.Max + 1 which == 2GB(1L << 31)
    Page(File directory, int id, long pageSize) {
        this.id = id;
        this.pageSize = pageSize;
        this.idxNextPage = (int) (pageSize - 4);
        this.file = new File(directory, String.valueOf(id));
        try {
            FileChannel channel = FileChannel.open(Paths.get(file.getAbsolutePath()),
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE);
            buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, pageSize);
            buffer.load();
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    int getId() {
        return id;
    }

    int remaining(int index) {
        return idxNextPage - index;
    }

    boolean hasRemaining(int index) {
        return remaining(index) > 0;
    }

    void write(int index, byte[] src) {
        // assert src.length <= remaining()
        write(index, src, 0, src.length);
    }

    void write(int index, byte[] src, int off, int len) {
        // assert len <= remaining()
        buffer.position(index);
        buffer.put(src, off, len);
    }

    void read(int index, byte[] dst) {
        // assert dst.length <= remaining()
        read(index, dst, 0, dst.length);
    }

    void read(int index, byte[] dst, int off, int len) {
        // assert len <= remaining()
        buffer.position(index);
        buffer.get(dst, off, len);
    }

    int getNextPage() {
        return buffer.getInt(idxNextPage);
    }

    void setNextPage(int nextPageId) {
        buffer.putInt(idxNextPage, nextPageId);
    }

    void release() {
        ByteBufferCleaner.clean(buffer);
        if (file.exists()) {
            if (!file.delete()) {
                throw new UncheckedIOException(
                        new IOException("Delete " + file.getAbsolutePath() + " failed"));
            }
        }
    }
}
