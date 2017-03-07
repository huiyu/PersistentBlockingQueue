package io.github.huiyu.collect;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import static org.junit.Assert.*;

public class ByteBufferCleanerTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testMappedBufferCleaner() throws Exception {
        File file = tempFolder.newFile();
        try (FileChannel channel = FileChannel.open(file.toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ)
        ) {
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, 1024L);
            buffer.load();
            assertTrue(buffer.isLoaded());
            ByteBufferCleaner.clean(buffer);
            assertFalse(buffer.isLoaded());
        }
    }
}