# PersistentBlockingQueue

[![Build Status](https://travis-ci.org/huiyu/persistent-blocking-queue.svg?branch=master)](https://travis-ci.org/huiyu/persistent-blocking-queue)

`PersistentBlockingQueue` is a persistent blocking queue backed by `MappedByteBuffer`.

## Basic Usage

`PersistentBlockingQueue` implements the `java.util.concurrent.BlockingQueue` interface, so you can use it like other blocking queues. 

Here is a brief example of how to use `PersistentBlockingQueue`:

```java
File file = new File("/path/to/queue/dir");
PersistentBlockingQueue<String> queue 
  = new PersistentBlockingQueue.Builder<String>(file).build();

queue.put("data");
queue.offer("new data");
String peek = queue.peek();
String poll = queue.poll();
queue.delete();
```

## Queue Builder

`PersistentBlockingQueue.Builder` provides some methods to configure the queue:

```java
PersistentBlockingQueue<String> queue = new PersistentBlockingQueue.Builder<String>(file)
        .capacity(1000)
        .serializer(Serializers.STRING_SERIALIZER)
        .pageSize(1 >> 30)
        .maxIdlePages(4)
        .build();
```

* `capacity(int capacity)`

Limit the number of queue items.

* `serializer(Serializer<E> serializer)`

Set the serializer of the queue item. 

Class `Serializers` contains some embedded serializers, `PersistentBlockingQueue` takes `Serializers.OBJECT_SERIALIZER` as default which uses `ObjectOutputStream` and `ObjectInputStream` to encode and decode objects. Users are best to implement their own serilizers to improve some performance.

* `pageSize(long pageSize)` 

Set the size of page.

Page is an abstraction of  `MappedByteBuffer`. `PersistentBlockingQueue` uses page to persist data.

* `maxIdlePages(int maxIdlePages)`

Limit the max number of idle pages.

Page's acquisition and release are costly, `PersistentBlockingQueue` will hold and resuse released pages.



