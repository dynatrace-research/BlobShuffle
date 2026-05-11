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

/**
 * Cache for byte array objects, keyed by object path.
 *
 * @see InMemoryBlobCache
 */
public interface BlobCache {

    /**
     * Returns the cached byte array for the given object path, or {@code null} if not present.
     *
     * @param objectPath the object key
     * @return the cached bytes, or {@code null}
     */
    byte[] getObject(String objectPath);

    /**
     * Inserts or updates a cached object.
     *
     * @param objectPath the object key
     * @param object the byte data to cache
     */
    void putObject(String objectPath, byte[] object);

    /**
     * Performs any pending maintenance operations, such as flushing deferred evictions.
     * Implementations backed by asynchronous eviction (e.g. Caffeine) should override
     * this to run synchronous cleanup; the default is a no-op.
     */
    default void cleanUp() {}

}
