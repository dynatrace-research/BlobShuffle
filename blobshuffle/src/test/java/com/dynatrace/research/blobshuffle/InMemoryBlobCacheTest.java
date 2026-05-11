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
