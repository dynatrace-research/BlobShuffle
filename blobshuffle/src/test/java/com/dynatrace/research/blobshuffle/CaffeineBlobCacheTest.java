package com.dynatrace.research.blobshuffle;

class CaffeineBlobCacheTest extends BlobCacheTest {

    @Override
    BlobCache createCache(int capacity) {
        return new CaffeineBlobCache(capacity);
    }

}
