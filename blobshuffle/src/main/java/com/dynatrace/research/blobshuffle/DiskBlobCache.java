package com.dynatrace.research.blobshuffle;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Two-level LRU cache that keeps recently accessed entries in an inner {@link BlobCache}
 * (memory tier) and spills additional entries to disk (disk tier).
 *
 * <p>The disk directory is wiped on construction and on {@link #close()}, making this an
 * ephemeral cache suitable for local EBS-backed scratch space.
 *
 * <p>Disk entries are stored as plain files under {@code diskDir} using the object path
 * as the relative file path (S3 keys are assumed to be filesystem-safe). File I/O is
 * synchronous; all public methods are thread-safe via the disk-map lock.
 */
public class DiskBlobCache implements BlobCache {

    private record DiskEntry(Path path, int length) {}

    private final BlobCache memoryTier;
    private final Path diskDir;
    private final Map<String, DiskEntry> diskMap;

    /**
     * Creates a new two-level cache.
     *
     * @param memoryTier    the in-memory cache tier (e.g. {@link InMemoryBlobCache})
     * @param diskDir       directory used for disk-tier files; created if absent and cleared on construction
     * @param maxDiskEntries maximum number of entries in the disk tier before LRU eviction
     */
    public DiskBlobCache(final BlobCache memoryTier, final Path diskDir, final int maxDiskEntries) {
        this.memoryTier = memoryTier;
        this.diskDir = diskDir;

        clearDirectory(diskDir);

        this.diskMap = Collections.synchronizedMap(
                new LinkedHashMap<>(maxDiskEntries + 1, 1.01f, true) {
                    @Override
                    protected boolean removeEldestEntry(final Map.Entry<String, DiskEntry> eldest) {
                        if (super.size() > maxDiskEntries) {
                            deleteFile(eldest.getValue().path());
                            return true;
                        }
                        return false;
                    }
                });
    }

    @Override
    public byte[] getObject(final String objectPath) {
        byte[] bytes = memoryTier.getObject(objectPath);
        if (bytes != null) {
            return bytes;
        }

        DiskEntry entry = diskMap.get(objectPath);
        if (entry == null) {
            return null;
        }

        try {
            bytes = Files.readAllBytes(entry.path());
        } catch (NoSuchFileException e) {
            // Raced with eviction -> treat as a cache miss
            diskMap.remove(objectPath);
            return null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        memoryTier.putObject(objectPath, bytes);
        return bytes;
    }

    @Override
    public void putObject(final String objectPath, final byte[] object) {
        memoryTier.putObject(objectPath, object);

        Path filePath = diskDir.resolve(objectPath);
        try {
            Files.createDirectories(filePath.getParent());
            Files.write(filePath, object, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        diskMap.put(objectPath, new DiskEntry(filePath, object.length));
    }

    @Override
    public void cleanUp() {
        memoryTier.cleanUp();
    }

    /**
     * Deletes the disk directory and all its contents.
     */
    public void close() {
        deleteRecursively(diskDir);
    }

    private static void clearDirectory(final Path dir) {
        if (Files.exists(dir)) {
            deleteRecursively(dir);
        }
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void deleteRecursively(final Path path) {
        try {
            if (Files.isDirectory(path)) {
                try (var stream = Files.list(path)) {
                    stream.forEach(DiskBlobCache::deleteRecursively);
                }
            }
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void deleteFile(final Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
