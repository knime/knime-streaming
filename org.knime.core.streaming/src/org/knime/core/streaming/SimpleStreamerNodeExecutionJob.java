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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.workflow.ConnectionContainer;
import org.knime.core.node.workflow.ConnectionProgress;
import org.knime.core.node.workflow.ConnectionProgressEvent;
import org.knime.core.node.workflow.NativeNodeContainer;
import org.knime.core.node.workflow.NodeContainer;
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
import org.knime.core.streaming.inoutput.AbstractOutputCache;
import org.knime.core.streaming.inoutput.InMemoryRowCache;
import org.knime.core.streaming.inoutput.NonTableOutputCache;
import org.knime.core.streaming.inoutput.NullOutputCache;
import org.knime.core.util.Pair;

/**
 * Job that streams a {@link SubNodeContainer}. Only streaming, no parallelization.
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
            Thread t = new Thread(r, "Streaming-" + THREAD_ID.getAndIncrement() + "-IDLE");
            t.setUncaughtExceptionHandler(UNCAUGHT_EXCEPTION_HANDLER);
            return t;
        }
    });

    /** The thread performing {@link #mainExecute()} - will be interrupted when canceled. */
    private Thread m_mainThread;

    /** Creates new job.
     * @param nc Node to stream, must be a {@link SubNodeContainer} (fails later on otherwise).
     * @param data Its input data.
     */
    public SimpleStreamerNodeExecutionJob(final NodeContainer nc, final PortObject[] data) {
        super(nc, data);
    }

    /** {@inheritDoc}
     * @return <code>false</code>. */
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
        for (NodeContainer nc : allNodeContainers) {
            nc.setNodeMessage(NodeMessage.NONE);
        }

        Map<NodeIDWithOutport, AbstractOutputCache<? extends PortObjectSpec>> connectionCaches;
        try {
            connectionCaches = prepareConnectionCaches(nodeContainer, wfm, allNodeContainers);
        } catch (WrappedNodeExecutionStatusException e) {
            return e.getStatus();
        }

        final Map<NodeContainer, SingleNodeStreamer> createStreamers;
        try {
            createStreamers = createStreamers(wfm, allNodeContainers, connectionCaches);
        } catch (WrappedNodeExecutionStatusException e) {
            return e.getStatus();
        }

        List<Pair<NodeContainer, Future<Void>>> nodeThreadList = new ArrayList<>(allNodeContainers.size());
        ExecutorCompletionService<Void> completionService =
                new ExecutorCompletionService<Void>(STREAMING_EXECUTOR_SERVICE);

        for (Map.Entry<NodeContainer, SingleNodeStreamer> e : createStreamers.entrySet()) {
            nodeThreadList.add(Pair.create(e.getKey(), completionService.submit(e.getValue().newCallable())));
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
     * @param nodeContainer
     * @param wfm
     * @param allNodeContainers
     * @return
     */
    private Map<NodeIDWithOutport, AbstractOutputCache<? extends PortObjectSpec>> prepareConnectionCaches(
        final NodeContainer nodeContainer, final WorkflowManager wfm, final Collection<NodeContainer> allNodeContainers)
                throws WrappedNodeExecutionStatusException {
        Map<NodeIDWithOutport, AbstractOutputCache<? extends PortObjectSpec>> connectionCaches = new LinkedHashMap<>();
        for (NodeContainer nc : allNodeContainers) {
            if (!(nc instanceof NativeNodeContainer)) {
                String message = "Subnodes must only contain native nodes in order to be streamed: "
                        + nc.getNameWithID();
                nodeContainer.setNodeMessage(NodeMessage.newError(message));
                LOGGER.error(message);
                throw new WrappedNodeExecutionStatusException(NodeContainerExecutionStatus.FAILURE);
            }
            NativeNodeContainer nnc = (NativeNodeContainer)nc;
            final boolean isDiamondStart = isDiamondStart(wfm, nnc);
            for (int op = 0; op < nc.getNrOutPorts(); op++) {
                PortType portType = nc.getOutPort(op).getPortType();
                final boolean isData = BufferedDataTable.TYPE.equals(portType)
                        || BufferedDataTable.TYPE_OPTIONAL.equals(portType);
                int nrStreamedConsumers = 0;
                boolean doStage = false;
                Set<ConnectionContainer> ccs = wfm.getOutgoingConnectionsFor(nc.getID(), op);
                for (ConnectionContainer cc : ccs) {
                    NodeID dest = cc.getDest();
                    int destPort = cc.getDestPort();
                    NativeNodeContainer destNode = wfm.getNodeContainer(dest, NativeNodeContainer.class, false);
                    if (destNode == null) { // could be a meta node or so -- assume false
                        doStage = true;
                    } else if (destPort == 0) {
                        assert !isData : "no data port at index 0";
                    } else {
                        if (isData && destNode.getNodeModel().getInputPortRoles()[destPort - 1].isStreamable()) {
                            nrStreamedConsumers += 1;
                        } else {
                            doStage = true;
                        }
                    }
                    ConnectionProgress p = new ConnectionProgress(nrStreamedConsumers > 0, isData ? "0" : "");
                    cc.progressChanged(new ConnectionProgressEvent(cc, p));
                }
                AbstractOutputCache<? extends PortObjectSpec> outputCache;
                if (isData) {
                    outputCache = new InMemoryRowCache(nnc, nnc.createExecutionContext(),
                        nrStreamedConsumers, doStage, isDiamondStart);
                } else {
                    outputCache = new NonTableOutputCache(nnc);
                }
                connectionCaches.put(new NodeIDWithOutport(nc.getID(), op), outputCache);
            }
        }
        return connectionCaches;
    }

    private boolean isDiamondStart(final WorkflowManager wfm, final NativeNodeContainer nnc) {
        // assert Thread.holdsLock(wfm.getWorkflowMutex());
        final HashSet<NodeID> visitedDownstreamNodes = new HashSet<>();
        final HashSet<NodeID> downstreamNodes = new HashSet<>();
        for (ConnectionContainer cc : wfm.getOutgoingConnectionsFor(nnc.getID())) {
            downstreamNodes.clear();
            collectDepthFirst(wfm, cc, downstreamNodes);
            int oldSize = visitedDownstreamNodes.size();
            visitedDownstreamNodes.addAll(downstreamNodes);
            if (visitedDownstreamNodes.size() < oldSize + downstreamNodes.size()) {
                // nnc is branching down-stream and those branches are merged again later
                return true;
            }
        }
        return false;
    }

    private void collectDepthFirst(final WorkflowManager wfm, final ConnectionContainer cc, final HashSet<NodeID> set) {
        set.add(cc.getDest());
        for (ConnectionContainer nextLevelOutCC : wfm.getOutgoingConnectionsFor(cc.getDest())) {
            collectDepthFirst(wfm, nextLevelOutCC, set);
        }
    }

    private Map<NodeContainer, SingleNodeStreamer> createStreamers(final WorkflowManager wfm,
        final Collection<NodeContainer> allNodeContainers,
        final Map<NodeIDWithOutport, AbstractOutputCache<? extends PortObjectSpec>> connectionCaches)
                throws WrappedNodeExecutionStatusException {
        Map<NodeContainer, SingleNodeStreamer> resultMap = new LinkedHashMap<>();
        for (NodeContainer nc : allNodeContainers) {
            final NativeNodeContainer nnc = (NativeNodeContainer)nc;
            final int nrIns = nnc.getNrInPorts();
            final int nrOuts = nnc.getNrOutPorts();
            final NodeID id = nnc.getID();

            AbstractOutputCache<? extends PortObjectSpec>[] outputCaches = new AbstractOutputCache[nrOuts];
            for (int i = 0; i < nrOuts; i++) {
                outputCaches[i] = connectionCaches.get(new NodeIDWithOutport(id, i));
                CheckUtils.checkState(outputCaches[i] != null, "No output cache for node %s, port %d", id, i);
            }

            AbstractOutputCache<? extends PortObjectSpec>[] upStreamCaches = new AbstractOutputCache[nrIns];
            for (int i = 0; i < upStreamCaches.length; i++) {
                PortType inportType = nnc.getInPort(i).getPortType();
                ConnectionContainer cc = wfm.getIncomingConnectionFor(id, i);
                if (cc == null) {
                    if (!inportType.isOptional()) {
                        throw new WrappedNodeExecutionStatusException(NodeContainerExecutionStatus.newFailure(
                            String.format("Node %s not fully connected", nnc.getNameWithID())));
                    } else {
                        upStreamCaches[i] = NullOutputCache.INSTANCE;
                    }
                } else {
                    AbstractOutputCache<? extends PortObjectSpec> upStreamCache =
                            connectionCaches.get(new NodeIDWithOutport(cc.getSource(), cc.getSourcePort()));
                    CheckUtils.checkState(upStreamCache != null, "No cache object for input %d at node %s",
                            cc.getDestPort(), id);
                    upStreamCaches[i] = upStreamCache;
                }
            }
            resultMap.put(nnc, new SingleNodeStreamer(nnc, outputCaches, upStreamCaches));
        }
        return resultMap;
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

    static final class NodeIDWithOutport {
        private final NodeID m_nodeID;
        private final int m_index;

        NodeIDWithOutport(final NodeID nodeID, final int index) {
            m_nodeID = CheckUtils.checkArgumentNotNull(nodeID, "Arg must not be null");
            m_index = index;
        }
        int getIndex() {
            return m_index;
        }
        NodeID getNodeID() {
            return m_nodeID;
        }
        @Override
        public int hashCode() {
            return m_nodeID.hashCode() + m_index;
        }
        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof NodeIDWithOutport)) {
                return false;
            }
            NodeIDWithOutport other = (NodeIDWithOutport)obj;
            return m_nodeID.equals(other.m_nodeID) && m_index == other.m_index;
        }
        @Override
        public String toString() {
            return "<" + m_nodeID + " - " + m_index + ">";
        }
    }

}
