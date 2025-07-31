package com.dynatrace.research.s3shuffler;

import java.util.SplittableRandom;
import java.util.function.Supplier;

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
