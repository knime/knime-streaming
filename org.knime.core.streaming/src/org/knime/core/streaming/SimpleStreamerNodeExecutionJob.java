/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   Mar 10, 2015 (wiswedel): created
 */
package org.knime.core.streaming;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.ArrayUtils;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.streamable.PartitionInfo;
import org.knime.core.node.streamable.StreamableOperator;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.workflow.ConnectionContainer;
import org.knime.core.node.workflow.NativeNodeContainer;
import org.knime.core.node.workflow.NodeContainer;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.node.workflow.NodeExecutionJob;
import org.knime.core.node.workflow.NodeID;
import org.knime.core.node.workflow.NodeMessage;
import org.knime.core.node.workflow.NodeMessage.Type;
import org.knime.core.node.workflow.SubNodeContainer;
import org.knime.core.node.workflow.WorkflowManager;
import org.knime.core.node.workflow.execresult.NodeContainerExecutionStatus;
import org.knime.core.node.workflow.execresult.SingleNodeContainerExecutionResult;
import org.knime.core.node.workflow.execresult.SubnodeContainerExecutionResult;
import org.knime.core.node.workflow.execresult.WorkflowExecutionResult;
import org.knime.core.streaming.inoutput.InMemoryRowCache;
import org.knime.core.streaming.inoutput.InMemoryRowInput;
import org.knime.core.streaming.inoutput.InMemoryRowOutput;
import org.knime.core.util.Pair;

/**
 *
 * @author Bernd Wiswedel, KNIME.com, Zurich, Switzerland
 */
public final class SimpleStreamerNodeExecutionJob extends NodeExecutionJob {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SimpleStreamerNodeExecutionJob.class);

    private static final UncaughtExceptionHandler UNCAUGHT_EXCEPTION_HANDLER = new UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(final Thread t, final Throwable e) {
            LOGGER.error("Uncaught " + e.getClass().getSimpleName() + " in thread " + t.getName(), e);
        }
    };

    static final ExecutorService STREAMING_EXECUTOR_SERVICE = Executors.newCachedThreadPool(new ThreadFactory() {
            final AtomicInteger THREAD_ID = new AtomicInteger();
        @Override
        public Thread newThread(final Runnable r) {
            Thread t = new Thread(r, "Streaming-" + THREAD_ID.getAndIncrement());
            t.setUncaughtExceptionHandler(UNCAUGHT_EXCEPTION_HANDLER);
            return t;
        }
    });

    /** The thread performing {@link #mainExecute()} - will be interrupted when canceled. */
    private Thread m_mainThread;

    /**
     * @param nc
     * @param data
     */
    public SimpleStreamerNodeExecutionJob(final NodeContainer nc, final PortObject[] data) {
        super(nc, data);
    }

    /** {@inheritDoc} */
    @Override
    protected boolean isReConnecting() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    protected NodeContainerExecutionStatus mainExecute() {
        m_mainThread = Thread.currentThread();
        try {
            return mainExecuteInternal();
        } finally {
            m_mainThread = null;
        }
    }

    private NodeContainerExecutionStatus mainExecuteInternal() {
        NodeContainer nodeContainer = getNodeContainer();
        if (!(nodeContainer instanceof SubNodeContainer)) {
            String message = "Streaming exeuction only available for subnodes";
            nodeContainer.setNodeMessage(new NodeMessage(Type.ERROR, message));
            LOGGER.error(message);
            return NodeContainerExecutionStatus.FAILURE;
        }
        SubNodeContainer subnodeContainer = (SubNodeContainer)nodeContainer;
        WorkflowManager wfm = subnodeContainer.getWorkflowManager();
        Collection<NodeContainer> allNodeContainers = wfm.getNodeContainers();
        HashMap<Pair<NodeID, Integer>, InMemoryRowCache> connectionCaches;
        try {
            connectionCaches = prepareConnectionCaches(nodeContainer, wfm, allNodeContainers);
        } catch (WrappedNodeExecutionStatusException e1) {
            return e1.getStatus();
        }
        List<Pair<NodeContainer, Future<Void>>> nodeThreadList = new ArrayList<>(allNodeContainers.size());
        ExecutorCompletionService<Void> completionService =
                new ExecutorCompletionService<Void>(STREAMING_EXECUTOR_SERVICE);
        try {
            submitJobs(wfm, allNodeContainers, connectionCaches, nodeThreadList, completionService);
        } catch (WrappedNodeExecutionStatusException e1) {
            return e1.getStatus();
        }

        String failMessage = null;
        boolean isFailed = false;
        boolean isCanceled = false;

        for (int i = 0; i < nodeThreadList.size(); i++) {
            try {
                completionService.take().get();
            } catch (InterruptedException | ExecutionException e) {
                // error handling done below
                if (e instanceof InterruptedException) {
                    isCanceled = true;
                } else {
                    isFailed = true;
                }
                break;
            }
        }

        WorkflowExecutionResult wfmExecResult = new WorkflowExecutionResult(wfm.getID());
        for (Pair<NodeContainer, Future<Void>> nodeFuture : nodeThreadList) {
            NodeContainer innerNC = nodeFuture.getFirst();
            Future<Void> future = nodeFuture.getSecond();
            SingleNodeContainerExecutionResult innerExecResult = new SingleNodeContainerExecutionResult();
            if ((isFailed || isCanceled) && !future.isDone()) {
                future.cancel(true);
                // assert future.isDone() -- according to API
            }
            try {
                future.get();
                innerExecResult.setSuccess(!(isFailed || isCanceled));
            } catch (InterruptedException | CancellationException e) {
                assert !(e instanceof InterruptedException) : "InterruptedException not expected as future is canceled";
                isCanceled = true;
                innerExecResult.setSuccess(false);
            } catch (ExecutionException e) {
                isFailed = true;
                failMessage = innerNC.getNameWithID() + " failed: " + e.getMessage();
                innerExecResult.setSuccess(false);
                NodeMessage innerMessage = new NodeMessage(Type.ERROR, e.getMessage());
                innerNC.setNodeMessage(innerMessage);
                innerExecResult.setMessage(innerMessage);
                LOGGER.error("Streaming thread to " + innerNC.getNameWithID() + " failed: " + e.getMessage(), e);
            }
            wfmExecResult.addNodeExecutionResult(innerNC.getID(), innerExecResult);
        }
        final boolean success = !(isFailed || isCanceled);
        wfmExecResult.setSuccess(success);
        SubnodeContainerExecutionResult execResult = new SubnodeContainerExecutionResult(subnodeContainer.getID());
        execResult.setWorkflowExecutionResult(wfmExecResult);

        NodeMessage message = NodeMessage.NONE;
        if (isFailed) {
            message = NodeMessage.newError(failMessage);
        } else if (isCanceled) {
            message = NodeMessage.newWarning("Execution canceled");
        }
        execResult.setSuccess(success);
        execResult.setMessage(message);
        subnodeContainer.setNodeMessage(message);
        return execResult;
    }

    /**
     * @param wfm
     * @param allNodeContainers
     * @param connectionCaches
     * @param nodeThreadList
     * @param completionService
     */
    private void submitJobs(final WorkflowManager wfm, final Collection<NodeContainer> allNodeContainers,
        final HashMap<Pair<NodeID, Integer>, InMemoryRowCache> connectionCaches,
        final List<Pair<NodeContainer, Future<Void>>> nodeThreadList,
        final ExecutorCompletionService<Void> completionService)
                throws WrappedNodeExecutionStatusException {
        for (NodeContainer nc : allNodeContainers) {
            final NativeNodeContainer nnc = (NativeNodeContainer)nc;
            // collect incoming caches
            final InMemoryRowInput[]  inCaches = new InMemoryRowInput[nnc.getNrInPorts() - 1];
            for (int i = 0; i < inCaches.length; i++) {
                ConnectionContainer cc = wfm.getIncomingConnectionFor(nnc.getID(), i + 1);
                if (cc == null) {
                    throw new WrappedNodeExecutionStatusException(NodeContainerExecutionStatus.FAILURE);
                }
                InMemoryRowCache imrc = connectionCaches.get(Pair.create(cc.getSource(), cc.getSourcePort() - 1));
                inCaches[i] = imrc.createRowInput(cc);
            }
            // collect outgoing caches
            final InMemoryRowOutput[]  outCaches = new InMemoryRowOutput[nnc.getNrOutPorts() - 1];
            for (int o = 0; o < outCaches.length; o++) {
                InMemoryRowCache imrc = connectionCaches.get(Pair.create(nnc.getID(), o));
                outCaches[o] = imrc.createRowOutput();
            }

            final PortObjectSpec[] inSpecs = ArrayUtils.remove(wfm.getNodeInputSpecs(nnc.getID()), 0);
            Future<Void> future = completionService.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    updateThreadName(nnc.getNameWithID());
                    NodeContext.pushContext(nnc);
                    try {
                        final StreamableOperator strop = nnc.getNodeModel().createStreamableOperator(
                            new PartitionInfo(0, 1), inSpecs);
                        ExecutionContext exec = nnc.createExecutionContext();
                        nnc.getNode().openFileStoreHandler(exec);
                        strop.runFinal(inCaches, outCaches, exec.createSubExecutionContext(0.0));
                        return null;
                    } finally {
                        NodeContext.removeLastContext();
                        for (InMemoryRowInput input : inCaches) {
                            input.close();
                        }
                        for (InMemoryRowOutput output : outCaches) {
                            try {
                                output.close();
                            } catch (Exception e) {
                                LOGGER.error("Failed to close output: " + e.getMessage(), e);
                            }
                        }
                    }
                }
            });
            nodeThreadList.add(Pair.create((NodeContainer)nnc, future));
        }
    }

    /**
     * @param nodeContainer
     * @param wfm
     * @param allNodeContainers
     * @return
     */
    private HashMap<Pair<NodeID, Integer>, InMemoryRowCache> prepareConnectionCaches(final NodeContainer nodeContainer,
        final WorkflowManager wfm, final Collection<NodeContainer> allNodeContainers)
                throws WrappedNodeExecutionStatusException {
        HashMap<Pair<NodeID, Integer>, InMemoryRowCache> connectionCaches
            = new LinkedHashMap<Pair<NodeID, Integer>, InMemoryRowCache>();
        for (NodeContainer nc : allNodeContainers) {
            if (!(nc instanceof NativeNodeContainer)) {
                String message = "Subnodes must only contain native nodes in order to be streamed: "
                        + nc.getNameWithID();
                nodeContainer.setNodeMessage(new NodeMessage(Type.ERROR, message));
                LOGGER.error(message);
                throw new WrappedNodeExecutionStatusException(NodeContainerExecutionStatus.FAILURE);
            }
            for (int op = 0; op < nc.getNrOutPorts() - 1; op++) {
                Set<ConnectionContainer> ccs = wfm.getOutgoingConnectionsFor(nc.getID(), op + 1);
                DataTableSpec spec = (DataTableSpec)(nc.getOutPort(op + 1).getPortObjectSpec());
                connectionCaches.put(Pair.create(nc.getID(), op), new InMemoryRowCache(ccs.size(), spec));
            }
        }
        return connectionCaches;
    }

    static void updateThreadName(final String nameSuffix) {
        String name = Thread.currentThread().getName();
        name = name.replaceAll("^(Streaming-\\d+).*", "$1-" + nameSuffix);
        Thread.currentThread().setName(name);
    }

    /** {@inheritDoc} */
    @Override
    protected boolean cancel() {
        Thread mainThread = m_mainThread;
        if (mainThread != null) {
            mainThread.interrupt();
            return true;
        }
        return false;
    }

    static final class WrappedNodeExecutionStatusException extends Exception {

        private final NodeContainerExecutionStatus m_status;

        WrappedNodeExecutionStatusException(final NodeContainerExecutionStatus status) {
            m_status = CheckUtils.checkArgumentNotNull(status, "Arg must not be null");
        }

        /** @return the status (non-null) */
        public NodeContainerExecutionStatus getStatus() {
            return m_status;
        }
    }

}
