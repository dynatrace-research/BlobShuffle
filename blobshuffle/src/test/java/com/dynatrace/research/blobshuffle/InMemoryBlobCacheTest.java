package com.dynatrace.research.blobshuffle;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryBlobCacheTest extends BlobCacheTest {

    @Override
    BlobCache createCache(int capacity) {
        return new InMemoryBlobCache(capacity);
    }

    @Test
    void lruEviction() {
        // InMemoryBlobCache uses a strict access-order LinkedHashMap, so LRU eviction order
        // is deterministic and can be asserted precisely.
        BlobCache cache = createCache(2);
        cache.putObject("a", new byte[]{1});
        cache.putObject("b", new byte[]{2});
        // access "a" so "b" becomes the LRU entry
        cache.getObject("a");
        cache.putObject("c", new byte[]{3});

        assertThat(cache.getObject("a")).isNotNull();
        assertThat(cache.getObject("c")).isNotNull();
        assertThat(cache.getObject("b")).isNull(); // "b" was LRU and should have been evicted
    }

}
