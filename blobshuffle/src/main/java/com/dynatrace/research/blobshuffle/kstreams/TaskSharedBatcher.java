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
package com.dynatrace.research.blobshuffle.kstreams;

import com.dynatrace.research.blobshuffle.BatchCollector;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

// Configuration for batching to be used by different tasks, but not only from the same StreamsThread!
// TODO Move methods from processor here, but wait to see what async stuff should be reused in debatcher
public class TaskSharedBatcher {

    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
    //private final ExecutorService executorService = Executors.newFixedThreadPool(10);

    private final Map<String, BatchCollector> batchCollectors = new HashMap<>();

    private final BlockingQueue<AsyncResult<NotificationSet>> finishedAsyncCalls = new LinkedBlockingQueue<>();

    private final Map<String, Object> pendingEvents = new HashMap<>();

    private int pendingEventsC = 0;

    private long lastCommitTimestamp = System.currentTimeMillis();

    public TaskSharedBatcher() {}

    public BlockingQueue<AsyncResult<NotificationSet>> getFinishedAsyncCalls() {
        return finishedAsyncCalls;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public Map<String, BatchCollector> getBatchCollectors() {
        return batchCollectors;
    }

    public Map<String, Object> getPendingEvents() {
        return pendingEvents;
    }

    public int getPendingEventsC() {
        return pendingEventsC;
    }

    public void incPendingEventsC() {
        pendingEventsC++;
    }

    public void decPendingEventsC() {
        pendingEventsC--;
    }

    public long getLastCommitTimestamp() {
        return lastCommitTimestamp;
    }

    public void setLastCommitTimestamp(long lastCommitTimestamp) {
        this.lastCommitTimestamp = lastCommitTimestamp;
    }
}
