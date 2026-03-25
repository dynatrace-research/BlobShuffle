package com.dynatrace.research.blobshuffle;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Interface for periodic refresh tasks, such as refreshing the partition-to-rack mapping.
 */
public interface Refresher extends Closeable {

    /** A no-op refresher that performs no action. */
    Refresher NOOP = new NoopRefresher();

    /**
     * Creates a refresher that periodically executes the given callback.
     *
     * @param refreshInterval the interval between executions
     * @param callback the action to execute periodically
     * @return a new refresher
     */
    static Refresher create(Duration refreshInterval, Runnable callback) {
        return new OwnSingleThreadRefresher(refreshInterval, callback);
    }

    /** Starts the periodic refresh schedule. */
    void start();

    /** Executes the refresh callback once immediately. */
    void runOnce();

    @Override
    void close();

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


        @Override
        public void close() {
            this.executor.close();
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

        @Override
        public void close() {
            // Do nothing
        }
    }

}
