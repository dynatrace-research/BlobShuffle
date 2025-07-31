package com.dynatrace.research.s3shuffler.kstreams;

import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorNode;

public abstract class AbstractCommitListenerProcessor<KIn, VIn, KOut, VOut> extends ContextualProcessor<KIn, VIn, KOut, VOut> {

    private final ProcessorFlushStoreRegistry processorFlushStoreRegistry;

    //protected KeyValueStore<Bytes, byte[]> dummyStore;

    protected ProcessorNode<?, ?, ?, ?> thisNode;

    public AbstractCommitListenerProcessor(ProcessorFlushStoreRegistry processorFlushStoreRegistry) {
        super();
        this.processorFlushStoreRegistry = processorFlushStoreRegistry;
    }

    @Override
    public void init(ProcessorContext<KOut, VOut> context) {
        super.init(context);
        //this.dummyStore = context().getStateStore("DUMMY-DEBATCH");
        this.processorFlushStoreRegistry.setCallbackForTaskId(context().taskId(), this::callPreCommitCallback);

        InternalProcessorContext<KOut,VOut> internalContext = (InternalProcessorContext<KOut,VOut>) context();
        this.thisNode = internalContext.currentNode();
    }

    @Override
    public void close() {
        super.close();
    }

    private void callPreCommitCallback() {
        // Make sure to set the correct context
        // The following is copied from, for example, SessionCacheFlushListener
        InternalProcessorContext<KOut,VOut> internalContext = (InternalProcessorContext<KOut,VOut>) context();
        ProcessorNode<?, ?, ?, ?> prevNode = internalContext.currentNode();
        internalContext.setCurrentNode(this.thisNode);
        try {
            this.preCommit();
        } finally {
            internalContext.setCurrentNode(prevNode);
        }
    }

    protected abstract void preCommit();
}
