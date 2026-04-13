package com.dynatrace.research.blobshuffle.kstreams;

import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds per-task pre-commit callbacks registered by {@link AbstractCommitListenerProcessor}
 * instances. {@link FlushListenerStore#flushCache()} calls back into this registry to
 * trigger each processor's pre-commit logic at the right point in the Kafka Streams commit
 * lifecycle.
 */
public class ProcessorFlushStoreRegistry {

    // TODO can probably be optimized with a two-layered design.
    private final Map<TaskId, Runnable> callbacks = new ConcurrentHashMap<>();

    /**
     * Registers a pre-commit callback for the given task.
     *
     * @param taskId   the Kafka Streams task identifier
     * @param callback the callback to invoke before the commit
     */
    public void setCallbackForTaskId(TaskId taskId, Runnable callback) {
        this.callbacks.put(taskId, callback);
    }

    /**
     * Returns the pre-commit callback for the given task.
     *
     * @param taskId the Kafka Streams task identifier
     * @return the registered callback
     * @throws RuntimeException if no callback has been registered for the given task
     */
    public Runnable getCallbackForTaskId(TaskId taskId) {
        Runnable callback = this.callbacks.get(taskId);
        if (callback == null) {
            throw new RuntimeException("No callback for taskId " + taskId);
        }
        return callback;
    }

}
