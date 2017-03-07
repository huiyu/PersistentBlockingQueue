package io.github.huiyu.collect;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A persistent {@link BlockingQueue} backed by {@link MappedByteBuffer}.
 *
 * @param <E> the type of elements held in this queue
 */
public class PersistentBlockingQueue<E> extends AbstractQueue<E> implements BlockingQueue<E> {

    private static final String INDEX_NAME = ".index";

    private final File directory;
    private final Serializer<E> serializer;

    final ReentrantLock lock = new ReentrantLock();
    final Condition notFull = lock.newCondition();
    final Condition notEmpty = lock.newCondition();

    final Index index;
    final PageAllocator allocator;

    protected PersistentBlockingQueue(File file,
                                      Serializer<E> serializer,
                                      int capacity,
                                      long pageSize,
                                      int maxIdlePages) {
        this.directory = file;
        this.serializer = serializer;
        this.allocator = new PageAllocator(directory, maxIdlePages, pageSize);

        try {
            File indexFile = new File(file, INDEX_NAME);
            if (!file.exists()) {
                if (!file.mkdirs()) // try make dirs
                    throw new IOException("Can't create directory: " + file.getName());
                this.index = new Index(indexFile, capacity);
            } else if (file.list() != null && file.list().length != 0) {
                if (!indexFile.exists()) {
                    String msg = file.getName() + " is already exist and is not a persistent queue";
                    throw new IllegalArgumentException(msg);
                }
                this.index = new Index(indexFile);
            } else {
                this.index = new Index(indexFile, capacity);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int size() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return index.getSize();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void put(E e) throws InterruptedException {
        byte[] data = serializer.encode(Preconditions.checkNotNull(e));
        final ReentrantLock lock = this.lock;
        final Index index = this.index;
        lock.lockInterruptibly();
        try {
            while (index.getSize() == index.getCapacity())
                notFull.await();
            enqueue(data);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        byte[] data = serializer.encode(Preconditions.checkNotNull(e));
        long nanos = unit.toNanos(timeout);
        final ReentrantLock lock = this.lock;
        final Index index = this.index;
        lock.lockInterruptibly();
        try {
            while (index.getSize() == index.getCapacity()) {
                if (nanos <= 0)
                    return false;
                nanos = notFull.awaitNanos(nanos);
            }
            enqueue(data);
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean offer(E e) {
        byte[] data = serializer.encode(Preconditions.checkNotNull(e));
        final ReentrantLock lock = this.lock;
        final Index index = this.index;
        lock.lock();
        try {
            if (index.getSize() == index.getCapacity()) {
                return false;
            } else {
                enqueue(data);
                return true;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E take() throws InterruptedException {
        final ReentrantLock lock = this.lock;
        final Index index = this.index;
        byte[] data;
        lock.lockInterruptibly();
        try {
            while (index.getSize() == 0)
                notEmpty.await();
            data = dequeue();
        } finally {
            lock.unlock();
        }
        return serializer.decode(data);
    }

    @Override
    public E poll() {
        final ReentrantLock lock = this.lock;
        final Index index = this.index;
        byte[] data = null;
        lock.lock();
        try {
            if (index.getSize() != 0) {
                data = dequeue();
            }
        } finally {
            lock.unlock();
        }
        return data == null ? null : serializer.decode(data);
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        final Index index = this.index;
        final ReentrantLock lock = this.lock;
        byte[] data;
        lock.lockInterruptibly();
        try {
            while (index.getSize() == 0) {
                if (nanos <= 0)
                    return null;
                nanos = notEmpty.awaitNanos(nanos);
            }
            data = dequeue();
        } finally {
            lock.unlock();
        }
        return serializer.decode(data);
    }

    @Override
    public E peek() {
        final ReentrantLock lock = this.lock;
        final Index index = this.index;
        byte[] data;
        lock.lock();
        try {
            if (index.getSize() == 0)
                return null;

            Position pos = index.getHead();
            byte[] lenData = new byte[4];
            Position newPos = read(lenData, pos);
            data = new byte[Serializers.INTEGER_SERIALIZER.decode(lenData)];
            read(data, newPos); // ignore new position
        } finally {
            lock.unlock();
        }
        return serializer.decode(data);
    }

    @Override
    public int remainingCapacity() {
        final ReentrantLock lock = this.lock;
        final Index index = this.index;
        lock.lock();
        try {
            return index.getCapacity() - index.getSize();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        Preconditions.checkNotNull(c);
        Preconditions.checkArgument(c != this);
        if (maxElements <= 0) return 0;

        final Index index = this.index;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            int n = Math.min(maxElements, index.getSize());
            int i = 0;
            try {
                while (i < n) {
                    byte[] data = dequeue();
                    E e = serializer.decode(data);
                    c.add(e);
                    i++;
                }
                return n;
            } finally {
                if (i > 0) {
                    notFull.signal();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    protected void enqueue(byte[] data) {
        // assert lock.getHoldCount() == 1
        write(Serializers.INTEGER_SERIALIZER.encode(data.length));
        write(data);
        Index index = this.index;
        index.setSize(index.getSize() + 1);
        notEmpty.signal();
    }

    protected void write(byte[] src) {
        int length = src.length;
        int offset = 0;
        Index index = this.index;
        Page tail = index.getTail().page;
        int tailOffset = index.getTailOffset();
        while (offset < length) {
            int available = tail.remaining(tailOffset);
            int remaining = length - offset;
            if (available < remaining) {
                tail.write(tailOffset, src, offset, available);
                Page next = allocator.acquire();
                tail.setNextPage(next.getId());
                tail = next;
                tailOffset = 0;
                offset += available;
            } else {
                tail.write(tailOffset, src, offset, remaining);
                offset += remaining;
                tailOffset += remaining;
            }
        }

        index.setTailFile(tail.getId());
        index.setTailOffset(tailOffset);
    }

    protected byte[] dequeue() {
        // assert lock.getHoldCount() == 1
        Position head = index.getHead();

        Position pos = head;
        byte[] lenData = new byte[4];
        pos = read(lenData, pos);
        int dataLen = Serializers.INTEGER_SERIALIZER.decode(lenData);
        byte[] dst = new byte[dataLen];
        pos = read(dst, pos);

        // release page
        Page last = head.page;
        while (last.getId() != pos.page.getId()) {
            allocator.release(last.getId());
            last = allocator.acquire(last.getNextPage());
        }

        // update position
        Index index = this.index;
        index.setHeadFile(pos.page.getId());
        index.setHeadOffset(pos.offset);
        index.setSize(index.getSize() - 1);

        notFull.signal();
        return dst;
    }

    protected Position read(byte[] dst, Position pos) {
        Page page = pos.page;
        int pageOff = pos.offset;

        int dstLen = dst.length;
        int dstOff = 0;
        while (dstOff < dstLen) {
            int pageAvailable = page.remaining(pageOff);
            int dstRemain = dstLen - dstOff;
            if (pageAvailable < dstRemain) {
                page.read(pageOff, dst, dstOff, pageAvailable);
                Page next = allocator.acquire(page.getNextPage());
                page = next;
                pageOff = 0;
                dstOff += pageAvailable;
            } else {
                page.read(pageOff, dst, dstOff, dstRemain);
                dstOff += dstRemain;
                pageOff += dstRemain;
            }
        }
        return new Position(page, pageOff);
    }

    @Override
    public Iterator<E> iterator() {
        return new Itr();
    }

    public void delete() {
        allocator.pages.values().forEach(page -> ByteBufferCleaner.clean(page.buffer));
        ByteBufferCleaner.clean(index.buffer);
        try {
            Files.walkFileTree(directory.toPath(), new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                        throws IOException {
                    Files.deleteIfExists(file);
                    return FileVisitResult.CONTINUE;
                }
            });
            Files.deleteIfExists(directory.toPath());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static class Builder<E> {

        private static final long MIN_PAGE_SIZE = 1L << 19; // 512KB
        private static final long MAX_PAGE_SIZE = 1L << 31; // 2GB 
        private static final int MAX_IDLE_PAGES = 16;

        private final File file;
        private int capacity = Integer.MAX_VALUE;
        private Serializer<E> serializer = (Serializer<E>) Serializers.OBJECT_SERIALIZER;
        private long pageSize = 1 << 27; // default page size is 128MB

        private int maxIdlePages = MAX_IDLE_PAGES;

        public Builder(File file) {
            this.file = file;
        }

        public Builder<E> capacity(int capacity) {
            if (capacity < 0)
                throw new IllegalArgumentException("Capacity must >= 0");
            this.capacity = capacity;
            return this;
        }

        public Builder<E> serializer(Serializer<E> serializer) {
            if (serializer == null)
                throw new NullPointerException();
            this.serializer = serializer;
            return this;
        }

        public Builder<E> pageSize(long pageSize) {
            if (pageSize < MIN_PAGE_SIZE || pageSize > MAX_PAGE_SIZE)
                throw new IllegalArgumentException(
                        "Page size must >= " + MIN_PAGE_SIZE + " and <= " + MAX_PAGE_SIZE);
            this.pageSize = pageSize;
            return this;
        }

        public Builder<E> maxIdlePages(int maxIdlePages) {
            if (maxIdlePages < 0)
                throw new IllegalArgumentException("Max idle pages must >= 0");
            this.maxIdlePages = maxIdlePages;
            return this;
        }

        public PersistentBlockingQueue<E> build() {
            return new PersistentBlockingQueue<>(file, serializer, capacity, pageSize, maxIdlePages);
        }
    }


    class Itr implements Iterator<E> {

        private volatile Position curPos;

        Itr() {
            lock.lock();
            try {
                curPos = index.getHead();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public boolean hasNext() {
            lock.lock();
            try {
                return curPos.page.getId() == index.getTailFile()
                        && curPos.offset == index.getTailOffset();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public E next() {
            lock.lock();
            try {
                byte[] data = new byte[4];
                Position newPos = read(data, curPos);
                data = new byte[Serializers.INTEGER_SERIALIZER.decode(data)];
                newPos = read(data, newPos);
                curPos = newPos;
                return serializer.decode(data);
            } finally {
                lock.unlock();
            }
        }
    }

    static class Position {
        final Page page;
        final int offset;

        Position(Page page, int offset) {
            this.page = page;
            this.offset = offset;
        }
    }

    class Index {

        static final int LENGTH = 24;
        static final int IDX_SIZE = 0;
        static final int IDX_CAPACITY = 4;
        static final int IDX_HEAD_FILE = 8;
        static final int IDX_HEAD_OFFSET = 12;
        static final int IDX_TAIL_FILE = 16;
        static final int IDX_TAIL_OFFSET = 20;

        private FileChannel channel;
        private MappedByteBuffer buffer;

        Index(File file) throws IOException {
            this(file, Integer.MAX_VALUE);
        }

        // assert new index file
        Index(File file, int capacity) throws IOException {
            boolean isNew = false;
            if (!file.exists()) {
                if (file.createNewFile()) {
                    isNew = true;
                } else {
                    throw new IOException("File " + file.getAbsolutePath() + " can't be created");
                }
            }
            openFile(file);
            if (isNew) {
                buffer.putInt(IDX_CAPACITY, capacity);
            }
        }

        private void openFile(File file) throws IOException {
            channel = FileChannel.open(
                    Paths.get(file.getAbsolutePath()),
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, LENGTH);
            buffer.load();
            channel.close();
        }

        int getSize() {
            return buffer.getInt(IDX_SIZE);
        }

        void setSize(int size) {
            buffer.putInt(IDX_SIZE, size);
        }

        int getCapacity() {
            return buffer.getInt(IDX_CAPACITY);
        }

        int getHeadFile() {
            return buffer.getInt(IDX_HEAD_FILE);
        }

        void setHeadFile(int file) {
            buffer.putInt(IDX_HEAD_FILE, file);
        }

        int getHeadOffset() {
            return buffer.getInt(IDX_HEAD_OFFSET);
        }

        void setHeadOffset(int offset) {
            buffer.putInt(IDX_HEAD_OFFSET, offset);
        }

        int getTailFile() {
            return buffer.getInt(IDX_TAIL_FILE);
        }

        void setTailFile(int file) {
            buffer.putInt(IDX_TAIL_FILE, file);
        }

        int getTailOffset() {
            return buffer.getInt(IDX_TAIL_OFFSET);
        }

        void setTailOffset(int offset) {
            buffer.putInt(IDX_TAIL_OFFSET, offset);
        }

        Position getHead() {
            return new Position(allocator.acquire(getHeadFile()), getHeadOffset());
        }

        Position getTail() {
            return new Position(allocator.acquire(getTailFile()), getTailOffset());
        }
    }
}
