package com.dynatrace.research.blobshuffle;

import java.util.SplittableRandom;
import java.util.function.Supplier;

/**
 * Generates unique, time-ordered object identifiers for object storage batches.
 * Each identifier is composed of a hex-encoded timestamp followed by a random hex suffix.
 */
public class ObjectIdSupplier implements Supplier<String> {

    private final SplittableRandom randomNumberGenerator = new SplittableRandom();

    public ObjectIdSupplier() {}

    @Override
    public String get() {
        String currentTimestamp = Long.toHexString(System.currentTimeMillis());
        String random = Integer.toHexString(this.randomNumberGenerator.nextInt());
        return currentTimestamp + '-' + random;
    }
}
