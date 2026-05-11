/*
 * Copyright (c) 2026 Dynatrace LLC. All rights reserved.
 *
 * This software and associated documentation files (the "Software") are being
 * made available by Dynatrace LLC for the sole purpose of illustrating the
 * implementation of certain algorithms which are published. Permission is
 * hereby granted, free of charge, to any person obtaining a copy of the
 * Software, to view and use the Software for internal, non-production,
 * non-commercial purposes only. Without limiting the foregoing, the Software
 * may not (i) be used to process live data or train, fine-tune, enrich or
 * improve any machine learning or foundation model or other artificial
 * intelligence model or system or (ii) distributed, sublicensed, modified, used
 * to provide a service, or sold either alone or as part of or in combination
 * with any other software. The Software shall at all times be considered the
 * proprietary property of Dynatrace LLC.
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
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
