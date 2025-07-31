package com.dynatrace.research.s3shuffler.kstreams;

import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ProcessorFlushStoreRegistry {

    // TODO can probably be optimized with a two-layered design.
    private final Map<TaskId, Runnable> callbacks = new ConcurrentHashMap<>();

    public void setCallbackForTaskId(TaskId taskId, Runnable callback) {
        this.callbacks.put(taskId, callback);
    }

    public Runnable getCallbackForTaskId(TaskId taskId) {
        Runnable callback = this.callbacks.get(taskId);
        if (callback == null) {
            throw new RuntimeException("No callback for taskId " + taskId);
        }
        return callback;
    }

}
