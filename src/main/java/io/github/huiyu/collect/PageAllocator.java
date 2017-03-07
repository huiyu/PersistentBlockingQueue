package io.github.huiyu.collect;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;

class PageAllocator {

    private final Random ran = new Random();

    final File directory;
    final long pageSize;
    final int maxIdle;
    final LinkedList<Integer> idlePageIds;
    final Map<Integer, Page> pages;

    PageAllocator(File directory, int maxIdle, long pageSize) {
        this.directory = directory;
        this.maxIdle = maxIdle;
        this.pageSize = pageSize;
        this.idlePageIds = new LinkedList<>();
        pages = new HashMap<>();
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> idlePageIds.forEach(id -> pages.get(id).release())));
    }

    Page acquire() {
        if (idlePageIds.isEmpty()) {
            Page page = create();
            pages.put(page.getId(), page);
            return page;
        } else {
            Integer pageId = idlePageIds.poll();
            return pages.get(pageId);
        }
    }

    Page acquire(int id) {
        return pages.computeIfAbsent(id, k -> new Page(directory, id, pageSize));
    }

    void release(int id) {
        int size = idlePageIds.size();
        if (size >= maxIdle) {
            Page page = pages.get(id);
            page.release();
        } else {
            idlePageIds.push(id);
        }
    }

    private Page create() {
        // create new page
        int id;
        do {
            id = ran.nextInt(Integer.MAX_VALUE);
        } while (new File(directory, String.valueOf(id)).exists());
        return new Page(directory, id, pageSize);
    }
}
