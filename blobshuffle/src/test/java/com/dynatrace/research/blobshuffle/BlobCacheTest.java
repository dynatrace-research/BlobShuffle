package com.dynatrace.research.blobshuffle;

import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Abstract base test for all {@link BlobCache} implementations. Subclasses provide
 * a specific cache instance via {@link #createCache(int)} and inherit all common tests.
 */
abstract class BlobCacheTest {

    abstract BlobCache createCache(int capacity);

    @Test
    void missReturnsNull() {
        BlobCache cache = createCache(2);
        assertThat(cache.getObject("absent")).isNull();
    }

    @Test
    void putAndGet() {
        BlobCache cache = createCache(2);
        byte[] data = {1, 2, 3};
        cache.putObject("key", data);
        assertThat(cache.getObject("key")).isEqualTo(data);
    }

    @Test
    void capacityEnforced() {
        // After exceeding capacity, at most `capacity` entries should remain
        BlobCache cache = createCache(2);
        cache.putObject("a", new byte[]{1});
        cache.putObject("b", new byte[]{2});
        cache.putObject("c", new byte[]{3});
        cache.cleanUp(); // no-op for InMemoryBlobCache; flushes deferred evictions for Caffeine

        long present = Stream.of("a", "b", "c")
                .filter(k -> cache.getObject(k) != null)
                .count();
        assertThat(present).isEqualTo(2);
    }

    @Test
    void overwriteExistingEntry() {
        BlobCache cache = createCache(2);
        cache.putObject("key", new byte[]{1});
        cache.putObject("key", new byte[]{2});
        assertThat(cache.getObject("key")).isEqualTo(new byte[]{2});
    }

}

