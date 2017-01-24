package me.jeffreyu.collect;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.xml.crypto.Data;

import static me.jeffreyu.collect.Preconditions.*;
import static me.jeffreyu.collect.Serializers.INTEGER_SERIALIZER;

public class PersistentBlockingQueue<E> extends AbstractQueue<E> implements BlockingQueue<E> {

    private static final String INDEX_NAME = ".index";

    private final File directory;
    private final Serializer<E> serializer;

    final ReentrantLock lock = new ReentrantLock();
    final Condition notFull = lock.newCondition();
    final Condition notEmpty = lock.newCondition();

    final Index index;
    final PageAllocator allocator;

    volatile Page head;
    volatile Page tail;

    protected PersistentBlockingQueue(
            File file,
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
            initialize();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void initialize() {
        int headFile = this.index.getHeadFile();
        this.head = allocator.acquire(headFile);
        int tailFile = this.index.getTailFile();
        this.tail = allocator.acquire(tailFile);
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
        byte[] data = serializer.encode(checkNotNull(e));
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
        byte[] data = serializer.encode(checkNotNull(e));
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
        byte[] data = serializer.encode(checkNotNull(e));
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

            final Page[] pageHolder = new Page[]{this.head};
            final int[] pageOffsetHolder = new int[]{index.getHeadOffset()};

            Consumer<byte[]> reader = (byte[] dst) -> {

                Page page = pageHolder[0];
                int pageOffset = pageOffsetHolder[0];

                int dstOffset = 0;
                int dstLength = dst.length;
                while (dstOffset < dstLength) {
                    int available = page.remaining(pageOffset);
                    int remaining = dstLength - dstOffset;
                    if (available < remaining) {
                        page.read(pageOffset, dst, dstOffset, available);
                        int nextPageId = page.getNextPage();
                        page = allocator.acquire(nextPageId);
                        pageOffset = 0;
                        dstOffset += available;
                    } else {
                        page.read(pageOffset, dst, dstOffset, remaining);
                        dstOffset += remaining;
                        pageOffset = pageOffset + remaining;
                    }
                }

                // update
                pageHolder[0] = page;
                pageOffsetHolder[0] = pageOffset;
            };

            byte[] dst = new byte[4];
            reader.accept(dst);
            data = new byte[INTEGER_SERIALIZER.decode(dst)];
            reader.accept(data);
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
        checkNotNull(c);
        checkArgument(c != this);
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
        write(INTEGER_SERIALIZER.encode(data.length));
        write(data);
        Index index = this.index;
        index.setSize(index.getSize() + 1);
        notEmpty.signal();
    }

    protected void write(byte[] src) {
        int length = src.length;
        int offset = 0;
        Index index = this.index;
        Page tail = this.tail;
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

        this.tail = tail;
        index.setTailFile(tail.getId());
        index.setTailOffset(tailOffset);
    }

    protected byte[] dequeue() {
        byte[] dataLength = new byte[4];
        read(dataLength);
        int length = INTEGER_SERIALIZER.decode(dataLength);
        byte[] dst = new byte[length];
        read(dst);

        Index index = this.index;
        index.setSize(index.getSize() - 1);
        notFull.signal();
        return dst;
    }

    protected void read(byte[] dst) {
        // assert lock.getHoldCount() == 1
        int length = dst.length;
        Index index = this.index;
        Page head = this.head;
        int headOffset = index.getHeadOffset();
        int offset = 0;

        while (offset < length) {
            int available = head.remaining(headOffset);
            int remaining = length - offset;
            if (available < remaining) {
                head.read(headOffset, dst, offset, available);
                int nextPageId = head.getNextPage();
                Page next = allocator.acquire(nextPageId);
                allocator.release(head.getId());
                head = next;
                headOffset = 0;
                offset += available;
            } else {
                head.read(headOffset, dst, offset, remaining);
                offset += remaining;
                headOffset += remaining;
            }
        }
        this.head = head;
        index.setHeadFile(head.getId());
        index.setHeadOffset(headOffset);
    }


    @Override
    public Iterator<E> iterator() {
        return null;
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

    class Iter implements Iterator<E> {

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public E next() {
            return null;
        }
    }

    class Position {

        final int page;
        final long offset;

        Position(int page, long offset) {
            this.page = page;
            this.offset = offset;
        }

        int getPage() {
            return page;
        }

        long getOffset() {
            return offset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Position position = (Position) o;
            return page == position.page &&
                    offset == position.offset;
        }

        @Override
        public int hashCode() {
            return Objects.hash(page, offset);
        }
    }

    class Index implements Closeable {

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

        Position getHeader() {
            return new Position(getHeadFile(), getHeadOffset());
        }

        Position getTail() {
            return new Position(getTailFile(), getTailOffset());
        }

        @Override
        public void close() throws IOException {
            channel.close();
            ByteBufferCleaner.clean(buffer);
        }
    }
}
