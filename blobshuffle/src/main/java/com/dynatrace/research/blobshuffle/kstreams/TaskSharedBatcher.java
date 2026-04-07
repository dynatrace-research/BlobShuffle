package com.dynatrace.research.blobshuffle.kstreams;

import com.dynatrace.research.blobshuffle.AsyncResult;
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
