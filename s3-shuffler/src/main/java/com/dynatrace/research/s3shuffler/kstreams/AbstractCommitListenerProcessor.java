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
package com.dynatrace.research.s3shuffler.kstreams;

import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorNode;

/**
 * Abstract base class for processors that need to perform actions before a Kafka Streams commit.
 * <p>
 * Subclasses must implement the {@link #preCommit()} method, which is called before each commit
 * via the {@link ProcessorFlushStoreRegistry}. This class ensures that the correct processor
 * context is set when the pre-commit callback is invoked.
 *
 * @param <KIn>  the type of input keys
 * @param <VIn>  the type of input values
 * @param <KOut> the type of output keys
 * @param <VOut> the type of output values
 */
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

    /**
     * Called before each Kafka Streams commit. Subclasses must implement this method to perform
     * any necessary pre-commit actions.
     */
    protected abstract void preCommit();
}
