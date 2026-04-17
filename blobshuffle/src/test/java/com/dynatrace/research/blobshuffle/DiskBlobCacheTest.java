package com.dynatrace.research.blobshuffle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class DiskBlobCacheTest extends BlobCacheTest {

    @TempDir
    Path tempDir;

    @Override
    BlobCache createCache(int capacity) {
        // Memory tier holds 1 entry; disk tier holds `capacity` entries.
        // Items evicted from memory remain on disk, so the total observable capacity equals `capacity`.
        return new DiskBlobCache(new InMemoryBlobCache(1), tempDir.resolve("cache"), capacity);
    }

    @Test
    void diskTierServesValueEvictedFromMemory() {
        DiskBlobCache cache = new DiskBlobCache(new InMemoryBlobCache(1), tempDir.resolve("c1"), 4);
        cache.putObject("a", new byte[]{1});
        // Evict "a" from memory by inserting "b"
        cache.putObject("b", new byte[]{2});

        assertThat(cache.getObject("a")).isEqualTo(new byte[]{1});
    }

    @Test
    void fileDeletedOnDiskEviction() {
        Path diskDir = tempDir.resolve("c2");
        // disk tier holds 2 entries; "dir/a" should be evicted when "dir/c" is added
        DiskBlobCache cache = new DiskBlobCache(new InMemoryBlobCache(1), diskDir, 2);
        cache.putObject("dir/a", new byte[]{1});
        cache.putObject("dir/b", new byte[]{2});
        cache.putObject("dir/c", new byte[]{3});

        assertThat(diskDir.resolve("dir/a")).doesNotExist();
        assertThat(diskDir.resolve("dir/b")).exists();
        assertThat(diskDir.resolve("dir/c")).exists();
    }

    @Test
    void closeDeletesDiskDirectory() {
        Path diskDir = tempDir.resolve("c3");
        DiskBlobCache cache = new DiskBlobCache(new InMemoryBlobCache(1), diskDir, 4);
        cache.putObject("key", new byte[]{42});

        cache.close();

        assertThat(Files.exists(diskDir)).isFalse();
    }

    @Test
    void subdirsCreatedForNestedObjectPaths() {
        Path diskDir = tempDir.resolve("c4");
        DiskBlobCache cache = new DiskBlobCache(new InMemoryBlobCache(1), diskDir, 4);
        cache.putObject("a/b/c", new byte[]{7});

        assertThat(diskDir.resolve("a/b/c")).exists();
    }
}
