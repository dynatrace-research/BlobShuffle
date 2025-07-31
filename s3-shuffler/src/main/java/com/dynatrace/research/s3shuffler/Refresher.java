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
