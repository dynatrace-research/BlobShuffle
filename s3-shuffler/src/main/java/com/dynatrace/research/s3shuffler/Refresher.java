package com.dynatrace.research.s3shuffler;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public interface Refresher {

    Refresher NOOP = new NoopRefresher();

    static Refresher create(Duration refreshInterval, Runnable callback) {
        return new OwnSingleThreadRefresher(refreshInterval, callback);
    }

    void start();

    void runOnce();

    class OwnSingleThreadRefresher implements Refresher {

        private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        private final Duration refreshInterval;

        private final Runnable callback;

        private OwnSingleThreadRefresher(Duration refreshInterval, Runnable callback) {
            this.refreshInterval = refreshInterval;
            this.callback = callback;
        }

        @Override
        public void start() {
            executor.scheduleAtFixedRate(callback, refreshInterval.toSeconds(), refreshInterval.toSeconds(), TimeUnit.SECONDS);
        }

        @Override
        public void runOnce() {
            this.callback.run();
        }

    }

    class NoopRefresher implements Refresher {

        private NoopRefresher() {
        }

        @Override
        public void start() {
            // Do nothing
        }

        @Override
        public void runOnce() {
            // Do nothing
        }

    }

}
