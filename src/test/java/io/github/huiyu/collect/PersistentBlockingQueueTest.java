package io.github.huiyu.collect;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PersistentBlockingQueueTest {

    private final Random random = new Random();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testBuilder() throws Exception {
        File file = tempFolder.newFolder();
        PersistentBlockingQueue<String> queue = new PersistentBlockingQueue.Builder<String>(file)
                .build();
        assertTrue(file.exists());

        File index = new File(file, ".index");
        assertTrue(index.exists());

        assertEquals(0, queue.size());
        assertEquals(Integer.MAX_VALUE, queue.remainingCapacity());
    }

    @Test
    public void testBuilderWhenSetCapacity() throws Exception {
        File file = tempFolder.newFolder();
        PersistentBlockingQueue<String> queue = new PersistentBlockingQueue.Builder<String>(file)
                .capacity(1000)
                .build();
        assertEquals(0, queue.size());
        assertEquals(1000, queue.remainingCapacity());
    }

    @Test
    public void testIndex() throws Exception {
        File file = tempFolder.newFolder();
        PersistentBlockingQueue<String> queue = new PersistentBlockingQueue.Builder<String>(file)
                .build();
        PersistentBlockingQueue<String>.Index index = queue.index;
        assertEquals(Integer.MAX_VALUE, index.getCapacity());
        assertEquals(0, index.getSize());

        int size = 100;
        assertEquals(0, index.getSize());
        index.setSize(size);
        assertEquals(100, index.getSize());

        int headFileId = 1;
        int headFileOffset = 100;
        assertEquals(0, index.getHeadFile());
        assertEquals(0, index.getHeadOffset());
        index.setHeadFile(headFileId);
        index.setHeadOffset(headFileOffset);
        assertEquals(headFileId, index.getHeadFile());
        assertEquals(headFileOffset, index.getHeadOffset());

        int tailFileId = 2;
        int tailOffset = 200;
        assertEquals(0, index.getTailFile());
        assertEquals(0, index.getTailOffset());

        index.setTailFile(tailFileId);
        index.setTailOffset(tailOffset);
        assertEquals(tailFileId, index.getTailFile());
        assertEquals(tailOffset, index.getTailOffset());
    }


    @Test
    public void testWriteAndReadInOnePage() throws Exception {
        File file = tempFolder.newFolder();
        int pageSize = 1 << 19L;
        PersistentBlockingQueue<String> queue = new PersistentBlockingQueue.Builder<String>(file)
                .pageSize(pageSize)
                .serializer(Serializers.STRING_SERIALIZER)
                .build();
        PersistentBlockingQueue<String>.Index index = queue.index;

        ReentrantLock lock = queue.lock;
        lock.lock();

        try {
            byte[] bytesWrite = new byte[1024];
            random.nextBytes(bytesWrite);
            queue.enqueue(bytesWrite);
            assertEquals(index.getHeadFile(), index.getTailFile());
            assertEquals(0, index.getHeadOffset());
            assertEquals(1028, index.getTailOffset());

            byte[] bytesRead = queue.dequeue();
            assertArrayEquals(bytesWrite, bytesRead);
            assertEquals(index.getHeadFile(), index.getTailFile());
            assertEquals(1028, index.getHeadOffset());
            assertEquals(1028, index.getTailOffset());

            bytesWrite = new byte[512];
            random.nextBytes(bytesWrite);
            queue.enqueue(bytesWrite);
            assertEquals(index.getHeadFile(), index.getTailFile());
            assertEquals(1028, index.getHeadOffset());
            assertEquals(1028 + 512 + 4, index.getTailOffset());

            bytesRead = queue.dequeue();
            assertArrayEquals(bytesWrite, bytesRead);
            assertEquals(1028 + 512 + 4, index.getHeadOffset());
            assertEquals(1028 + 512 + 4, index.getTailOffset());
        } finally {
            lock.unlock();
        }
    }

    @Test
    public void testWriteAndReadCrossPages() throws Exception {
        File file = tempFolder.newFolder();
        int pageSize = 1 << 19L;
        PersistentBlockingQueue<String> queue = new PersistentBlockingQueue.Builder<String>(file)
                .pageSize(pageSize)
                .serializer(Serializers.STRING_SERIALIZER)
                .build();
        PersistentBlockingQueue<String>.Index index = queue.index;

        ReentrantLock lock = queue.lock;
        lock.lock();

        try {
            byte[] bytesWrite = new byte[pageSize];
            for (int i = 0; i < pageSize; i++) {
                bytesWrite[i] = 1;
            }

            queue.enqueue(bytesWrite);
            assertNotEquals(index.getHeadFile(), index.getTailFile());
            assertEquals(0, index.getHeadOffset());
            assertEquals(8, index.getTailOffset()); // 4 extra bytes for store next page id

            byte[] bytesRead = queue.dequeue();
            assertArrayEquals(bytesWrite, bytesRead);
            assertEquals(index.getHeadFile(), index.getTailFile());
            assertEquals(8, index.getHeadOffset());
            assertEquals(8, index.getTailOffset());
        } finally {
            lock.unlock();
        }
    }

    @Test
    public void testEnqueueAndDequeue() throws Exception {

        File file = tempFolder.newFolder();
        int pageSize = 1 << 19L; // 512KB
        PersistentBlockingQueue<byte[]> queue = new PersistentBlockingQueue.Builder<byte[]>(file)
                .serializer(Serializers.BYTE_ARRAY_SERIALIZER)
                .pageSize(pageSize)
                .build();

        ReentrantLock lock = queue.lock;
        lock.lock();

        try {

            byte[] enqueue = new byte[10];
            random.nextBytes(enqueue);
            queue.enqueue(enqueue);

            assertEquals(queue.index.getHeadFile(), queue.index.getTailFile());
            assertEquals(1, queue.size());
            assertEquals(Integer.MAX_VALUE - 1, queue.remainingCapacity());
            assertFalse(queue.isEmpty());

            byte[] dequeue = queue.dequeue();
            assertArrayEquals(enqueue, dequeue);
            assertEquals(0, queue.size());
            assertEquals(Integer.MAX_VALUE, queue.remainingCapacity());
            assertTrue(queue.isEmpty());

            // write multi byte arrays
            List<byte[]> byteArrays = new ArrayList<>();
            int times = 1000;
            for (int i = 0; i < times; i++) {
                byte[] byteArray = new byte[1024];
                random.nextBytes(byteArray);
                byteArrays.add(byteArray);
                queue.enqueue(byteArray);
            }
            assertEquals(times, queue.size());
            assertEquals(Integer.MAX_VALUE - times, queue.remainingCapacity());
            assertFalse(queue.isEmpty());

            List<byte[]> byteArraysRead = new ArrayList<>();
            for (int i = 0; i < times; i++) {
                byte[] byteArray = queue.dequeue();
                byteArraysRead.add(byteArray);
            }
            assertEquals(0, queue.size());
            assertEquals(Integer.MAX_VALUE, queue.remainingCapacity());
            assertTrue(queue.isEmpty());

            // check data
            for (int i = 0; i < times; i++) {
                assertArrayEquals(byteArrays.get(i), byteArraysRead.get(i));
            }
        } finally {
            lock.unlock();
        }
    }

    @Test
    public void testPut() throws Exception {
        File file = tempFolder.newFolder();
        PersistentBlockingQueue<byte[]> queue = new PersistentBlockingQueue.Builder<byte[]>(file)
                .serializer(Serializers.BYTE_ARRAY_SERIALIZER)
                .capacity(1)
                .build();

        byte[] data = new byte[1024];
        random.nextBytes(data);
        queue.put(data);

        assertEquals(1, queue.size());
        assertEquals(0, queue.remainingCapacity());

        // test blocking
        AtomicBoolean putted = new AtomicBoolean(false);
        new Thread(() -> {
            try {
                queue.put(data);
                putted.set(true);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

        Thread.sleep(10L);
        assertFalse(putted.get());
        queue.poll();

        Thread.sleep(10L);
        assertTrue(putted.get());
        assertEquals(1, queue.size());
        assertEquals(0, queue.remainingCapacity());
    }

    @Test
    public void testOffer() throws Exception {
        File file = tempFolder.newFolder();
        PersistentBlockingQueue<byte[]> queue = new PersistentBlockingQueue.Builder<byte[]>(file)
                .serializer(Serializers.BYTE_ARRAY_SERIALIZER)
                .capacity(1)
                .build();

        byte[] data = new byte[1024];
        random.nextBytes(data);
        assertTrue(queue.offer(data));
        assertFalse(queue.offer(data));

        // test time elapsed
        AtomicBoolean offered = new AtomicBoolean(false);
        long start = System.currentTimeMillis();
        offered.set(queue.offer(data, 500, TimeUnit.MILLISECONDS));
        long elapsed = System.currentTimeMillis() - start;
        assertFalse(offered.get());
        assertTrue(elapsed >= 500L);

        // test blocking
        offered.set(false);
        new Thread(() -> {
            try {
                boolean res = queue.offer(data, 1, TimeUnit.MINUTES);
                offered.set(res);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
        queue.poll();

        Thread.sleep(10L);
        assertTrue(offered.get());
        assertEquals(1, queue.size());
    }

    @Test
    public void testTake() throws Exception {
        File file = tempFolder.newFolder();
        PersistentBlockingQueue<byte[]> queue = new PersistentBlockingQueue.Builder<byte[]>(file)
                .serializer(Serializers.BYTE_ARRAY_SERIALIZER)
                .capacity(1)
                .build();

        byte[] data = new byte[1024];
        random.nextBytes(data);
        queue.put(data);

        assertEquals(1, queue.size());
        byte[] taken = queue.take();
        assertArrayEquals(data, taken);

        // test blocking
        AtomicReference<byte[]> ref = new AtomicReference<>();
        new Thread(() -> {
            try {
                ref.set(queue.take());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
        assertNull(ref.get());

        queue.put(data);
        Thread.sleep(10L);
        assertArrayEquals(data, ref.get());
    }

    @Test
    public void testPoll() throws Exception {
        File file = tempFolder.newFolder();
        PersistentBlockingQueue<byte[]> queue = new PersistentBlockingQueue.Builder<byte[]>(file)
                .serializer(Serializers.BYTE_ARRAY_SERIALIZER)
                .capacity(1)
                .build();

        byte[] data = new byte[1024];
        random.nextBytes(data);
        queue.put(data);
        assertEquals(1, queue.size());

        byte[] polled = queue.poll();
        assertArrayEquals(data, polled);
        assertNull(queue.poll());

        // test blocking
        long start = System.currentTimeMillis();
        assertNull(queue.poll(500L, TimeUnit.MILLISECONDS));
        long elapsed = System.currentTimeMillis() - start;
        assertTrue(elapsed >= 500L);

        AtomicReference<byte[]> ref = new AtomicReference<>();
        new Thread(() -> {
            try {
                ref.set(queue.poll(1, TimeUnit.MINUTES));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

        assertNull(ref.get());

        queue.put(data);
        Thread.sleep(10L);
        assertArrayEquals(data, ref.get());
    }


    @Test
    public void testPeek() throws Exception {
        File file = tempFolder.newFolder();
        PersistentBlockingQueue<byte[]> queue = new PersistentBlockingQueue.Builder<byte[]>(file)
                .serializer(Serializers.BYTE_ARRAY_SERIALIZER)
                .build();

        // get null if queue is empty
        assertTrue(queue.isEmpty());
        assertNull(queue.peek());

        byte[] data = new byte[1024];
        random.nextBytes(data);
        queue.put(data);

        // get but not remove
        for (int i = 0; i < 10; i++) {
            byte[] peeked = queue.peek();
            assertArrayEquals(data, peeked);
        }
    }

    @Test
    public void testDrainTo() throws Exception {
        File file = tempFolder.newFolder();
        PersistentBlockingQueue<byte[]> queue = new PersistentBlockingQueue.Builder<byte[]>(file)
                .serializer(Serializers.BYTE_ARRAY_SERIALIZER)
                .build();

        byte[] data = new byte[1024];
        for (int i = 0; i < 100; i++) {
            random.nextBytes(data);
            queue.put(data);
        }
        assertEquals(100, queue.size());

        List<byte[]> drainTo = new ArrayList<>();
        int count = queue.drainTo(drainTo, 10);
        assertEquals(10, count);
        assertEquals(10, drainTo.size());
        assertEquals(90, queue.size());

        count = queue.drainTo(drainTo, 100);
        assertEquals(90, count);
        assertEquals(100, drainTo.size());
        assertTrue(queue.isEmpty());
    }

    @Test
    public void testIterator() throws Exception {
        File file = tempFolder.newFolder();
        PersistentBlockingQueue<byte[]> queue = new PersistentBlockingQueue.Builder<byte[]>(file)
                .serializer(Serializers.BYTE_ARRAY_SERIALIZER)
                .build();
        List<byte[]> datas = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            byte[] data = new byte[1024];
            random.nextBytes(data);
            datas.add(data);
            queue.put(data);
        }

        int i = 0;
        for (byte[] data : queue) {
            assertArrayEquals(datas.get(i++), data);
        }
    }
    
    @Test
    public void testDelete() throws Exception {
        File file = tempFolder.newFolder();
        PersistentBlockingQueue<byte[]> queue = new PersistentBlockingQueue.Builder<byte[]>(file)
                .serializer(Serializers.BYTE_ARRAY_SERIALIZER)
                .build();
        
        for (int i = 0; i < 100; i++) {
            byte[] data = new byte[1024];
            random.nextBytes(data);
            queue.put(data);
        }
        
        queue.delete();
        assertFalse(file.exists());
    }
}