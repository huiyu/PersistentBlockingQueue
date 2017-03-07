package io.github.huiyu.collect;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class PageAllocatorTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testCreate() throws Exception {
        PageAllocator allocator = new PageAllocator(tempFolder.newFolder(), 1, 1024L);
        Set<Integer> integers = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            Page page = allocator.acquire();
            assertFalse(integers.contains(page.getId()));
            integers.add(page.getId());
            assertTrue(page.getId() > 0);
        }
    }

    @Test
    public void testAcquireById() throws Exception {
        PageAllocator allocator = new PageAllocator(tempFolder.newFolder(), 1, 1024L);
        Page page1 = allocator.acquire();
        Page page2 = allocator.acquire(page1.getId());
        assertEquals(page1, page2);
    }
}