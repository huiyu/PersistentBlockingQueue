package me.jeffreyu.collect;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Random;

import static org.junit.Assert.*;

public class PageTest {

    private final Random random = new Random();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testPageBasic() throws Exception {
        File file = tempFolder.newFolder();
        Page page = new Page(file, 0, 1024L);
        assertTrue(page.buffer.isLoaded());
        assertEquals(new File(file, String.valueOf(0)), page.file);
        assertEquals(0, page.getId());

        page.setNextPage(1);
        assertEquals(1, page.getNextPage());

        assertEquals(1020, page.remaining(0));
        assertTrue(page.hasRemaining(0));
        assertTrue(page.hasRemaining(1019));
        assertFalse(page.hasRemaining(1020));

        page.release();
        assertFalse(page.buffer.isLoaded());
        assertFalse(page.file.exists());
    }

    @Test
    public void testPageReadAndWrite() throws Exception {
        File file = tempFolder.newFolder();
        Page page = new Page(file, 0, 1024L);
        // check 100 times
        for (int i = 0; i < 100; i++) {
            // write
            int pos = random.nextInt(page.remaining(0));
            byte[] bytesWrite = new byte[page.remaining(pos)];
            random.nextBytes(bytesWrite);
            page.write(pos, bytesWrite);
            assertEquals(0, page.getNextPage());
            // read
            byte[] bytesRead = new byte[page.remaining(pos)];
            page.read(pos, bytesRead);
            assertArrayEquals(bytesWrite, bytesRead);
        }
    }

    @Test
    public void testPageReload() throws Exception {
        File file = tempFolder.newFolder();
        Page page = new Page(file, 0, 1024L);

        byte[] bytesWrite = new byte[page.remaining(0)];
        random.nextBytes(bytesWrite);
        page.write(0, bytesWrite);

        Page reloadPage = new Page(file, 0, 1024L);
        assertTrue(reloadPage.buffer.isLoaded());
        assertEquals(page.getId(), reloadPage.getId());

        byte[] bytesRead = new byte[page.remaining(0)];
        reloadPage.read(0, bytesRead);
        assertArrayEquals(bytesWrite, bytesRead);
    }
}