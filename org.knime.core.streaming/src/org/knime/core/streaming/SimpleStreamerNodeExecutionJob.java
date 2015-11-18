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

import static org.knime.core.node.workflow.execresult.NodeContainerExecutionStatus.newFailure;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.exec.SandboxedNodeCreator;
import org.knime.core.node.exec.SandboxedNodeCreator.SandboxedNode;
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
import org.knime.core.node.workflow.WorkflowCreationHelper;
import org.knime.core.node.workflow.WorkflowEvent;
import org.knime.core.node.workflow.WorkflowManager;
import org.knime.core.node.workflow.WorkflowPersistor.LoadResult;
import org.knime.core.node.workflow.execresult.NativeNodeContainerExecutionResult;
import org.knime.core.node.workflow.execresult.NodeContainerExecutionResult;
import org.knime.core.node.workflow.execresult.NodeContainerExecutionStatus;
import org.knime.core.node.workflow.execresult.NodeExecutionResult;
import org.knime.core.node.workflow.execresult.SubnodeContainerExecutionResult;
import org.knime.core.node.workflow.execresult.WorkflowExecutionResult;
import org.knime.core.streaming.inoutput.AbstractOutputCache;
import org.knime.core.streaming.inoutput.InMemoryRowCache;
import org.knime.core.streaming.inoutput.NonTableOutputCache;
import org.knime.core.streaming.inoutput.NullOutputCache;
import org.knime.core.util.LockFailedException;
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

    /** A workflow parent into which the streamed sub-nodes are copied. These copies will then be executed and the
     * result is copied back into the original sub-node. This instance is created lazy. */
    private static WorkflowManager streamParentWFM;

    static final ExecutorService STREAMING_EXECUTOR_SERVICE = Executors.newCachedThreadPool(new ThreadFactory() {
            final AtomicInteger THREAD_ID = new AtomicInteger();
        @Override
        public Thread newThread(final Runnable r) {
            Thread t = new Thread(r, "Streaming-" + THREAD_ID.getAndIncrement() + "-IDLE");
            t.setUncaughtExceptionHandler(UNCAUGHT_EXCEPTION_HANDLER);
            return t;
        }
    });

    private static final PortObject[] EMPTY_PO_ARRAY = new PortObject[0];

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

    /**
     * @return the streamParentWFM
     */
    private synchronized static WorkflowManager getStreamParentWFM() {
        if (streamParentWFM == null) {
            LOGGER.debug("Creating streamed node parent workflow project");
            streamParentWFM = WorkflowManager.ROOT.createAndAddProject("Streamer-Subnode-Parent",
                new WorkflowCreationHelper());
            streamParentWFM.addListener(e -> checkAutoDiscardStreamWFM(e));
        }
        return streamParentWFM;
    }

    /**
     * @param e
     */
    private synchronized static void checkAutoDiscardStreamWFM(final WorkflowEvent e) {
        if (WorkflowEvent.Type.NODE_REMOVED.equals(e.getType())) {
            if (streamParentWFM.getNodeContainers().isEmpty()) {
                LOGGER.debug("Discarding streamed node parent workflow project");
                WorkflowManager.ROOT.removeProject(streamParentWFM.getID());
                streamParentWFM = null;
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    protected NodeContainerExecutionStatus mainExecute() {
        NodeContainer nodeContainer = getNodeContainer();
        if (!(nodeContainer instanceof SubNodeContainer)) {
            String message = "Streaming exeuction only available for subnodes";
            nodeContainer.setNodeMessage(new NodeMessage(Type.ERROR, message));
            LOGGER.error(message);
            return NodeContainerExecutionStatus.FAILURE;
        }

        final SubNodeContainer origContainer = (SubNodeContainer)nodeContainer;
        origContainer.setNodeMessage(NodeMessage.NONE);
        final WorkflowManager origWFM = origContainer.getWorkflowManager();
        origWFM.getNodeContainers().stream().filter(nc -> nc.getNodeContainerState().isExecutionInProgress())
        .forEach(nc -> nc.setNodeMessage(NodeMessage.NONE));
        m_mainThread = Thread.currentThread();
        final SandboxedNodeCreator sandBoxCreator = new SandboxedNodeCreator(origContainer, getPortObjects(),
            getStreamParentWFM()).setCopyData(false).setForwardConnectionProgressEvents(true);

        try (final SandboxedNode sandboxedNode = sandBoxCreator.createSandbox(new ExecutionMonitor())) {
            final SubNodeContainer runContainer = sandboxedNode.getSandboxNode(SubNodeContainer.class);
            final WorkflowManager runWFM = runContainer.getWorkflowManager();
            ExecutionContextCreator execCreator = new ExecutionContextCreator(origWFM, runWFM.getID());
            NodeContainerExecutionResult execResult = mainExecuteInternal(runContainer, execCreator);
            origContainer.loadExecutionResult(execResult, new ExecutionMonitor(), new LoadResult("Stream-Exec-Result"));
            return execResult;
        } catch (CanceledExecutionException e1) {
            origContainer.setNodeMessage(NodeMessage.newWarning("Canceled"));
            return NodeContainerExecutionStatus.newFailure("Canceled");
        } catch (InvalidSettingsException | IOException | LockFailedException e1) {
            final String msg = e1.getClass().getSimpleName() + " while preparing stream job: " + e1.getMessage();
            LOGGER.error(msg, e1);
            origContainer.setNodeMessage(NodeMessage.newError(msg));
            return NodeContainerExecutionStatus.newFailure(msg);
        } catch (WrappedNodeExecutionStatusException e) {
            origContainer.setNodeMessage(NodeMessage.newError(e.getMessage()));
            return e.getStatus();
        } finally {
            m_mainThread = null;
        }
    }

    private NodeContainerExecutionResult mainExecuteInternal(final SubNodeContainer runContainer,
        final ExecutionContextCreator execCreator) throws WrappedNodeExecutionStatusException {
        WorkflowManager wfm = runContainer.getWorkflowManager();
        Collection<NodeContainer> allNodeContainers = wfm.getNodeContainers();
        allNodeContainers.stream().forEach(nc -> nc.setNodeMessage(NodeMessage.NONE));

        final Map<NodeIDWithOutport, AbstractOutputCache<? extends PortObjectSpec>> connectionCaches =
                prepareConnectionCaches(wfm, allNodeContainers, execCreator);

        final Map<NodeContainer, SingleNodeStreamer> createStreamers =
                createStreamers(wfm, allNodeContainers, connectionCaches, execCreator);

        final List<Pair<NodeContainer, Future<NativeNodeContainerExecutionResult>>> nodeThreadList =
                new ArrayList<>(allNodeContainers.size());
        final ExecutorCompletionService<NativeNodeContainerExecutionResult> completionService =
                new ExecutorCompletionService<>(STREAMING_EXECUTOR_SERVICE);

        for (Map.Entry<NodeContainer, SingleNodeStreamer> e : createStreamers.entrySet()) {
            nodeThreadList.add(Pair.create(e.getKey(), completionService.submit(e.getValue().newCallable())));
        }

        String failMessage = null;
        boolean isFailed = false;
        boolean isCanceled = false;

        for (int i = 0; i < nodeThreadList.size(); i++) {
            try {
                NativeNodeContainerExecutionResult result = completionService.take().get();
                if (!result.isSuccess()) {
                    // error handling done below
                    isFailed = true;
                    break;
                }
            } catch (InterruptedException e) {
                isCanceled = true;
                break;
            } catch (ExecutionException e) {
                // shouldn't happen as the streamer doesn't throw an exception - but for sake of completeness and
                // to please the compiler...
                isFailed = true;
                break;
            }
        }

        WorkflowExecutionResult wfmExecResult = new WorkflowExecutionResult(wfm.getID());
        for (Pair<NodeContainer, Future<NativeNodeContainerExecutionResult>> nodeFuture : nodeThreadList) {
            NodeContainer innerNC = nodeFuture.getFirst();
            Future<NativeNodeContainerExecutionResult> future = nodeFuture.getSecond();
            NativeNodeContainerExecutionResult innerExecResult = new NativeNodeContainerExecutionResult();
            if ((isFailed || isCanceled) && !future.isDone()) {
                future.cancel(true);
                continue;
                // assert future.isDone() -- according to API
            }
            try {
                if (innerNC instanceof NativeNodeContainer) {
                    innerExecResult = future.get();
                    NativeNodeContainerExecutionResult nncExecResult = innerExecResult;
                    NodeExecutionResult nodeExecResult = nncExecResult.getNodeExecutionResult();
                    if (innerExecResult.isSuccess()) {
                        PortObjectSpec[] outSpecs = new PortObjectSpec[innerNC.getNrOutPorts()];
                        PortObject[] outObjects = new PortObject[outSpecs.length];
                        for (int o = 0; o < outSpecs.length; o++) {
                            AbstractOutputCache<? extends PortObjectSpec> outputCache =
                                    connectionCaches.get(new NodeIDWithOutport(innerNC.getID(), o));
                            outSpecs[o] = outputCache.getPortObjectSpec();
                            outObjects[o] = outputCache.getPortObjectMock();
                        }
                        nodeExecResult.setPortObjectSpecs(outSpecs);
                        nodeExecResult.setPortObjects(outObjects);
                    } else {
                        failMessage = innerNC.getName() + " failed";
                        if (innerExecResult.getNodeMessage().getMessageType().equals(NodeMessage.Type.ERROR)) {
                            failMessage = failMessage.concat(": " + innerExecResult.getNodeMessage().getMessage());
                        }
                    }
                } else {
                    assert false : innerNC.getClass().getSimpleName() + " does not support streaming";
                }
            } catch (InterruptedException e) {
                assert false : "InterruptedException not expected as future is canceled";
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
        SubnodeContainerExecutionResult execResult = new SubnodeContainerExecutionResult(runContainer.getID());
        execResult.setWorkflowExecutionResult(wfmExecResult);

        NodeMessage message = NodeMessage.NONE;
        if (isFailed) {
            message = NodeMessage.newError(failMessage);
        } else if (isCanceled) {
            message = NodeMessage.newWarning("Execution canceled");
        }
        execResult.setSuccess(success);
        execResult.setMessage(message);

        return execResult;
    }

    /**
     * @param wfm
     * @param allNodeContainers
     * @return
     */
    private Map<NodeIDWithOutport, AbstractOutputCache<? extends PortObjectSpec>> prepareConnectionCaches(
        final WorkflowManager wfm, final Collection<NodeContainer> allNodeContainers,
        final ExecutionContextCreator execCreator) throws WrappedNodeExecutionStatusException {
        Map<NodeIDWithOutport, AbstractOutputCache<? extends PortObjectSpec>> connectionCaches = new LinkedHashMap<>();
        for (NodeContainer nc : allNodeContainers) {
            if (!(nc instanceof NativeNodeContainer)) {
                String msg = "Subnodes must only contain native nodes in order to be streamed: " + nc.getNameWithID();
                LOGGER.error(msg);
                throw new WrappedNodeExecutionStatusException(msg, newFailure(msg));
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
                    outputCache = new InMemoryRowCache(nnc, execCreator.apply(nnc.getID()),
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
        final Map<NodeIDWithOutport, AbstractOutputCache<? extends PortObjectSpec>> connectionCaches,
        final ExecutionContextCreator execCreator) throws WrappedNodeExecutionStatusException {
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
                        final String msg = String.format("Node %s not fully connected", nnc.getNameWithID());
                        throw new WrappedNodeExecutionStatusException(msg, newFailure(msg));
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
            final ExecutionContext execContext = execCreator.apply(nnc.getID());
            resultMap.put(nnc, new SingleNodeStreamer(nnc, execContext, outputCaches, upStreamCaches));
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

        WrappedNodeExecutionStatusException(final String message, final NodeContainerExecutionStatus status) {
            super(message);
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

    /** Utility class that creates the ExecutionContext that is bound to nodes in the original workflow/subnode. */
    static final class ExecutionContextCreator implements java.util.function.Function<NodeID, ExecutionContext> {
        private final WorkflowManager m_origWFM;
        private final NodeID m_runContainerNodeID;
        private final Map<NodeID, ExecutionContext> m_origIDToRunExecContext;

        /**
         * @param origWFM The original workflow - will search in it for the original node
         * @param runContainerNodeID The ID of the node's runtime counterpart.
         */
        ExecutionContextCreator(final WorkflowManager origWFM, final NodeID runContainerNodeID) {
            m_origWFM = CheckUtils.checkArgumentNotNull(origWFM);
            m_runContainerNodeID = CheckUtils.checkArgumentNotNull(runContainerNodeID);
            m_origIDToRunExecContext = new HashMap<>();
        }

        /** For the given runtimeID find the corresponding node in the original workflow and create and return
         * its execution context.
         * @param childRunID the id of the node in the runtime instance.
         * @return the exec context bound to the original node - any data created on the exec context lives as long
         * as the node is executed.
         */
        @Override
        public synchronized ExecutionContext apply(final NodeID childRunID) {
            ExecutionContext ctx = m_origIDToRunExecContext.get(childRunID);
            if (ctx == null) {
                CheckUtils.checkArgument(childRunID.hasPrefix(m_runContainerNodeID),
                    "Not child of \"%s\": \"%s\"", m_runContainerNodeID, childRunID);
                Deque<Integer> indexStack = new LinkedList<>();
                NodeID toStrip = childRunID;
                do {
                    indexStack.addLast(toStrip.getIndex());
                    toStrip = toStrip.getPrefix();
                } while (!toStrip.equals(m_runContainerNodeID));

                NodeID toExtend = m_origWFM.getID();
                while (!indexStack.isEmpty()) {
                    toExtend = toExtend.createChild(indexStack.removeLast());
                }
                NodeContainer nc = m_origWFM.findNodeContainer(toExtend);
                CheckUtils.checkArgument(nc instanceof NativeNodeContainer, "Not a NativeNodeContainer: " + nc);
                ctx = ((NativeNodeContainer)nc).createExecutionContext();
                m_origIDToRunExecContext.put(childRunID, ctx);
            }
            return ctx;
        }

    }

}
