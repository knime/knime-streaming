/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
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
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.lang3.ArrayUtils;
import org.knime.core.data.DataRow;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.exec.SandboxedNodeCreator;
import org.knime.core.node.exec.SandboxedNodeCreator.SandboxedNode;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.flowvariable.FlowVariablePortObject;
import org.knime.core.node.port.flowvariable.FlowVariablePortObjectSpec;
import org.knime.core.node.streamable.DataTableRowInput;
import org.knime.core.node.streamable.InputPortRole;
import org.knime.core.node.streamable.MergeOperator;
import org.knime.core.node.streamable.OutputPortRole;
import org.knime.core.node.streamable.PartitionInfo;
import org.knime.core.node.streamable.PortInput;
import org.knime.core.node.streamable.PortOutput;
import org.knime.core.node.streamable.RowInput;
import org.knime.core.node.streamable.StreamableOperator;
import org.knime.core.node.streamable.StreamableOperatorInternals;
import org.knime.core.node.streamable.RowOutput.OutputClosedException;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.workflow.ConnectionContainer;
import org.knime.core.node.workflow.ConnectionProgress;
import org.knime.core.node.workflow.ConnectionProgressEvent;
import org.knime.core.node.workflow.FlowObjectStack;
import org.knime.core.node.workflow.NativeNodeContainer;
import org.knime.core.node.workflow.NodeContainer;
import org.knime.core.node.workflow.NodeExecutionJob;
import org.knime.core.node.workflow.NodeID;
import org.knime.core.node.workflow.NodeMessage;
import org.knime.core.node.workflow.NodeMessage.Type;
import org.knime.core.node.workflow.SingleNodeContainer;
import org.knime.core.node.workflow.SubNodeContainer;
import org.knime.core.node.workflow.WorkflowCreationHelper;
import org.knime.core.node.workflow.WorkflowEvent;
import org.knime.core.node.workflow.WorkflowLock;
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
import org.knime.core.streaming.inoutput.StagedTableRowInput;
import org.knime.core.util.LockFailedException;
import org.knime.core.util.Pair;

/**
 * Job that streams a {@link SubNodeContainer}. Only streaming, no parallelization.
 *
 * @author Bernd Wiswedel, KNIME AG, Zurich, Switzerland
 */
public final class SimpleStreamerNodeExecutionJob extends NodeExecutionJob {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SimpleStreamerNodeExecutionJob.class);

    /** A workflow parent into which the streamed sub-nodes are copied. These copies will then be executed and the
     * result is copied back into the original sub-node. This instance is created lazy. */
    private static WorkflowManager streamParentWFM;

    private static final PortObject[] EMPTY_PO_ARRAY = new PortObject[0];

    /** The thread performing {@link #mainExecute()} - will be interrupted when canceled. */
    private Thread m_mainThread;

    /** Cancelation flag - unfortunately m_mainThread.interrupt() isn't enough and honestly we didn't get to
     * the bottom of why. Cancellation is tested in
     * org.knime.core.node.workflow.BugAP5712_CloseWhileStreaming.testSaveLoadWhileExecuting(). */
    private boolean m_isCanceled;

    private final SimpleStreamerNodeExecutionSettings m_settings;

    /** Creates new job.
     * @param nc Node to stream, must be a {@link SubNodeContainer} (fails later on otherwise).
     * @param data Its input data.
     * @param settings ...
     */
    SimpleStreamerNodeExecutionJob(final NodeContainer nc, final PortObject[] data,
        final SimpleStreamerNodeExecutionSettings settings) {
        super(nc, data);
        m_settings = CheckUtils.checkArgumentNotNull(settings);
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
            if ((streamParentWFM != null) && streamParentWFM.getNodeContainers().isEmpty()) {
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
        if (!(nodeContainer instanceof SingleNodeContainer)) {
            String message = "Streaming execution is only available for single nodes (i.e. not metanodes).";
            nodeContainer.setNodeMessage(new NodeMessage(Type.ERROR, message));
            LOGGER.error(message);
            return NodeContainerExecutionStatus.FAILURE;
        } else if(nodeContainer instanceof SubNodeContainer) {
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
            } catch (CanceledExecutionException | InterruptedException e1) {
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
        } else if(nodeContainer instanceof NativeNodeContainer) {
            mainExecuteInternal((NativeNodeContainer) nodeContainer);
        } else {
            //should not happen
            //TODO better exception handling
            throw new RuntimeException("Not supported");
        }
    }

    private NodeContainerExecutionResult mainExecuteInternal(final SubNodeContainer runContainer,
        final ExecutionContextCreator execCreator) throws WrappedNodeExecutionStatusException {
        WorkflowManager wfm = runContainer.getWorkflowManager();
        Collection<NodeContainer> allNodeContainers = wfm.getNodeContainers();
        allNodeContainers.stream().forEach(nc -> nc.setNodeMessage(NodeMessage.NONE));

        final Map<NodeIDWithOutport, AbstractOutputCache<? extends PortObjectSpec>> connectionCaches =
                prepareConnectionCaches(wfm, allNodeContainers, execCreator);

        final Map<NodeContainer, SingleNodeStreamer> nodeToStreamerMap =
                createStreamers(wfm, allNodeContainers, connectionCaches, execCreator);

        final List<Pair<NodeContainer, Future<NodeContainerExecutionResult>>> nodeThreadList =
                new ArrayList<>(allNodeContainers.size());

        final BlockingQueue<Future<NodeContainerExecutionResult>> completionQueue = new LinkedBlockingQueue<Future<NodeContainerExecutionResult>>();

        for (Map.Entry<NodeContainer, SingleNodeStreamer> e : nodeToStreamerMap.entrySet()) {
            NodeContainer nc = e.getKey();
            nodeThreadList.add(Pair.create(e.getKey(), e.getValue().submitJob().getFuture()));
        }

        String failMessage = null;
        boolean isFailed = false;
        boolean isCanceled = m_isCanceled;

        for (int i = 0; i < nodeThreadList.size() && !isCanceled && !isFailed; i++) {
            try {
                NodeContainerExecutionResult result = completionQueue.take().get();
                if (!result.isSuccess()) {
                    // error handling done below
                    isFailed = true;
                }
            } catch (InterruptedException e) {
                isCanceled = true;
            } catch (ExecutionException e) {
                // shouldn't happen as the streamer doesn't throw an exception - but for sake of completeness and
                // to please the compiler...
                isFailed = true;
            }
        }

        WorkflowExecutionResult wfmExecResult = new WorkflowExecutionResult(wfm.getID());
        for (Pair<NodeContainer, Future<NodeContainerExecutionResult>> nodeFuture : nodeThreadList) {
            NodeContainer innerNC = nodeFuture.getFirst();
            Future<NodeContainerExecutionResult> future = nodeFuture.getSecond();
            NodeContainerExecutionResult innerExecResult = new NativeNodeContainerExecutionResult();
            if ((isFailed || isCanceled) && !future.isDone()) {
                future.cancel(true);
                continue;
                // assert future.isDone() -- according to API
            }
            try {
                if (innerNC instanceof NativeNodeContainer) {
                    innerExecResult = future.get();
                    NodeContainerExecutionResult nncExecResult = innerExecResult;
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
        if (!success) {
            // don't allow partial success - otherwise the user can re-run with half way executed flow but
            // the green nodes have no output table in their output
            wfmExecResult.getExecutionResultMap().values().stream().forEach(e -> e.setSuccess(false));
        }
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

    private NativeNodeContainerExecutionResult mainExecuteInternal(final NativeNodeContainer nnc) {
        final PortInput[] inputs = getPortInputs();
        final PortOutput[] outputs = getPortOutputs();

        final WorkflowManager parent = nnc.getParent();
        final NodeModel nM = nnc.getNodeModel();
        final InputPortRole[] inputPortRoles = ArrayUtils.add(
            nM.getInputPortRoles(), 0, InputPortRole.NONDISTRIBUTED_NONSTREAMABLE);

        //TODO!!!
//        final FlowObjectStack[] flowObjectStacks = new FlowObjectStack[m_upStreamCaches.length];

        final PortObjectSpec[] inSpecs = new PortObjectSpec[inputs.length];
        for (int i = 0; i < inSpecs.length; i++) {
            //TODO
            inSpecs[i] = inputs[i].getPortObjectSpec();
        }
        final PortObjectSpec[] inSpecsNoFlowPort = ArrayUtils.remove(inSpecs, 0);

        // can be null
        MergeOperator mergeOperator = nM.createMergeOperator();

        // never be null
        final StreamableOperator strop = nM.createStreamableOperator(new PartitionInfo(0, 1), inSpecsNoFlowPort);

        // pre-iterate if necessary
        StreamableOperatorInternals streamInternals = nM.createInitialStreamableOperatorInternals();
        BufferedDataTable[] stagedTables = null;
        while (nM.iterate(streamInternals)) {
            // create staged data tables (only for the streamable inputs)
            if (stagedTables == null) {
                stagedTables = new BufferedDataTable[inputs.length - 1];
                for (int i = 0; i < stagedTables.length; i++) {
                    if (inputPortRoles[i + 1].isStreamable()) {
                        RowInput rowInput = (RowInput)inputs[i + 1];
                        BufferedDataTable stagedTable = null;
                        if (rowInput instanceof StagedTableRowInput) {
                            stagedTable = ((StagedTableRowInput)rowInput).getTable();
                        } else if (rowInput != null) { // port can be optional
                            BufferedDataContainer cont =
                                m_execContext.createDataContainer(rowInput.getDataTableSpec());
                            DataRow r = null;
                            while ((r = rowInput.poll()) != null) {
                                cont.addRowToTable(r);
                                m_execContext.checkCanceled();
                            }
                            cont.close();
                            stagedTable = cont.getTable();
                        }
                        stagedTables[i] = stagedTable;
                    } else {
                        stagedTables[i] = null;
                    }
                }
            }

            // (re-)create streamable staged inputs
            for (int i = 0; i < stagedTables.length; i++) {
                if (stagedTables[i] != null) {
                    inputs[i + 1] = new DataTableRowInput(stagedTables[i]);
                }
            }
            strop.loadInternals(streamInternals);
            strop.runIntermediate(ArrayUtils.remove(inputs, 0), m_execContext);
            closeInputs(inputs);
            streamInternals = strop.saveInternals();

            if (mergeOperator != null) {
                streamInternals = mergeOperator.mergeIntermediate(new StreamableOperatorInternals[] {streamInternals});
            }
        }
        if (stagedTables != null) {
            // (re-)create streamable staged inputs for the last time
            // they might have possibly been closed within the iterate-loop above
            for (int i = 0; i < stagedTables.length; i++) {
                if (stagedTables[i] != null) {
                    inputs[i + 1] = new DataTableRowInput(stagedTables[i]);
                }
            }
        }

        boolean isConfigureOK;

        try (WorkflowLock lock = nnc.getParent().lock()) {
            nnc.getParent().createAndSetFlowObjectStackFor(nnc, flowObjectStacks);
            // need a final call to configure in order to propagate flow variables into the configuration
            //
            // this potentially has race conditions with streamed inports - it's not deterministic if an
            // upstream node has published all flow variables when this method gets call. It's deterministic
            // for non-streamable ports as their port objects are already queried/available (unless a node
            // pushes flow variables after setting the result)
            isConfigureOK = nnc.callNodeConfigure(inSpecs, true);
        }

        CheckUtils.checkSetting(isConfigureOK, "Configuration failed");

        PortObjectSpec[] outSpecsNoFlowPort = nM.computeFinalOutputSpecs(streamInternals, inSpecsNoFlowPort);
        if (outSpecsNoFlowPort != null) {
            m_outputCaches[0].setPortObjectSpec(FlowVariablePortObjectSpec.INSTANCE);
            CheckUtils.checkState(outSpecsNoFlowPort.length == nnc.getNrOutPorts() - 1, "Wrong number of "
                + "output ports: got %d, expected %d", outSpecsNoFlowPort.length, nnc.getNrOutPorts() - 1);
            for (int i = 0; i < outSpecsNoFlowPort.length; i++) {
                final PortObjectSpec s = outSpecsNoFlowPort[i];
                if (s != null) {
                    m_outputCaches[i + 1].setPortObjectSpec(s);
                }
            }
        }

        /* prepare outputs */
        IntStream.range(0, outputs.length).forEach(i -> outputs[i] = m_outputCaches[i].getPortOutput());
        //if all inputs a non-distributed, all outputs are provided here
        //otherwise only distributed outputs are non-null (the others are processed further
        //below by calling the StreamableOperator#finishStreamableExecution-method
        //see also documentation of the StreamableOperator#runFinal-method
        PortOutput[] distrOutputs = new PortOutput[outputs.length - 1]; //without flow variable port
        PortOutput[] nonDistrOutputs = new PortOutput[outputs.length - 1]; //without flow variable port
        if (IntStream.range(1, inputPortRoles.length).anyMatch(i -> inputPortRoles[i].isDistributable())) {
            //at least one input is distributable
            OutputPortRole[] outputPortRoles = nM.getOutputPortRoles();
            IntStream.range(1, outputs.length).forEach(i -> {
                if (outputPortRoles[i - 1].isDistributable()) {
                    distrOutputs[i - 1] = outputs[i];
                } else {
                    nonDistrOutputs[i - 1] = outputs[i];
                }
            });
        } else {
            //no input is distributable
            IntStream.range(0, distrOutputs.length).forEach(i -> {
                distrOutputs[i] = outputs[i + 1];
                nonDistrOutputs[i] = null;
            });
        }


        strop.loadInternals(streamInternals);
        nnc.getNode().openFileStoreHandler(m_execContext);
        try {
            strop.runFinal(ArrayUtils.remove(inputs, 0), distrOutputs,
                m_execContext.createSubExecutionContext(0.0));
        } catch (OutputClosedException oce) {
            LOGGER.debug("Early stopping of streaming operator as consumers are done");
        } finally {
            closeInputs(inputs);
            closeOutputs(distrOutputs);
            streamInternals = strop.saveInternals();
        }

        //run merge operation if available
        if (mergeOperator != null) {
            streamInternals = mergeOperator.mergeFinal(new StreamableOperatorInternals[]{streamInternals});
        }

        //if at least one input is distributable and a merge operator has been created this method must be implemented
        if (mergeOperator != null) {
            nM.finishStreamableExecution(streamInternals, m_execContext.createSubExecutionContext(0.0),
                nonDistrOutputs);
        }
        closeOutputs(nonDistrOutputs);

        ((NonTableOutputCache)m_outputCaches[0]).setObject(FlowVariablePortObject.INSTANCE);
        PortObject[] rawInput = Stream.of(m_upStreamCaches).map(
            c -> c.getPortObjectMock()).toArray(PortObject[]::new);
        PortObject[] rawOutput = Stream.of(m_outputCaches).map(
            c -> c.getPortObjectMock()).toArray(PortObject[]::new);
        nnc.getNode().assignInternalHeldObjects(rawInput, null, m_execContext, rawOutput);
        m_execContext.setMessage("Creating execution result");
        NativeNodeContainerExecutionResult executionResult =
                nnc.createExecutionResult(m_execContext.createSubProgress(0.0));
        executionResult.setSuccess(true);
        return executionResult;
    } catch (Exception e) {
        NativeNodeContainerExecutionResult r = new NativeNodeContainerExecutionResult();
        r.setNodeExecutionResult(new NodeExecutionResult());
        if (e instanceof InterruptedException) {
            // downstream error - ignore here
            LOGGER.debugWithFormat("interrupt received for \"%s\"- canceling streaming execution",
                nnc.getNameWithID());
        } else if (e instanceof InvalidSettingsException) {
            LOGGER.warn(e.getMessage(), e);
            r.setMessage(NodeMessage.newWarning(e.getMessage()));
        } else {
            String error = "(" + e.getClass().getSimpleName() + "): " + e.getMessage();
            LOGGER.error(error, e);
            r.setMessage(NodeMessage.newError(e.getMessage()));
        }
        r.setSuccess(false);
        return r;
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
            final NodeContainerCacheHandle ncCacheHandle = new NodeContainerCacheHandle(nnc);
            final boolean isDiamondStart = isDiamondStart(wfm, nnc);
            for (int op = 0; op < nc.getNrOutPorts(); op++) {
                PortType portType = nc.getOutPort(op).getPortType();
                final boolean isData = BufferedDataTable.TYPE.equals(portType)
                        || BufferedDataTable.TYPE_OPTIONAL.equals(portType);
                int nrStreamedConsumers = 0;
                boolean hasNonStreamableConsumer = false;
                Set<ConnectionContainer> ccs = wfm.getOutgoingConnectionsFor(nc.getID(), op);
                for (ConnectionContainer cc : ccs) {
                    NodeID dest = cc.getDest();
                    int destPort = cc.getDestPort();
                    NativeNodeContainer destNode = wfm.getNodeContainer(dest, NativeNodeContainer.class, false);
                    if (destNode == null) { // could be a meta node or so -- assume false
                        hasNonStreamableConsumer = true;
                    } else if (destPort == 0) {
                        assert !isData : "no data port at index 0";
                    } else {
                        if (isData && destNode.getNodeModel().getInputPortRoles()[destPort - 1].isStreamable()) {
                            nrStreamedConsumers += 1;
                        } else {
                            hasNonStreamableConsumer = true;
                        }
                    }
                    ConnectionProgress p = new ConnectionProgress(nrStreamedConsumers > 0, isData ? "0" : "");
                    cc.progressChanged(new ConnectionProgressEvent(cc, p));
                }
                AbstractOutputCache<? extends PortObjectSpec> outputCache;
                if (isData) {
                    ncCacheHandle.incrementTotalCacheCounter();
                    outputCache = new InMemoryRowCache(ncCacheHandle, execCreator.apply(nnc.getID()),
                        m_settings, nrStreamedConsumers, hasNonStreamableConsumer, isDiamondStart);
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
            if (nc.getNodeContainerState().isExecuted()) {
                final String msg = String.format("Wrapped Metanode must not contain executed"
                    + " nodes in order to be streamed (\"%s\" is executed)", nc.getNameWithID());
                throw new WrappedNodeExecutionStatusException(msg, newFailure(msg));
            }
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
            m_isCanceled = true;
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

    /** A common synchronization object for all {@link InMemoryRowCache} objects that are associated with the node's
     * output ports. The consumers of all outputs need to be done (RowInput#close called) before the node's execution
     * thread can early-abort. */
    public static final class NodeContainerCacheHandle {
        private final SingleNodeContainer m_nc;
        private final Set<InMemoryRowCache> m_closedCachesSet;
        private int m_totalOutputCount;

        NodeContainerCacheHandle(final SingleNodeContainer nc) {
            m_nc = CheckUtils.checkArgumentNotNull(nc);
            m_closedCachesSet = new HashSet<>();
        }

        /** @return the node container. */
        public SingleNodeContainer getSingleNodeContainer() {
            return m_nc;
        }
        /** Increment the number of expected output caches. Called before execution starts. */
        void incrementTotalCacheCounter() {
            assert m_closedCachesSet.isEmpty() : "Already closing outputs, can't increment total count";
            m_totalOutputCount += 1;
        }

        /** Called by the {@link InMemoryRowCache} after all its consumers are closed. It remembers the completed
         * output caches. If it's the last output cache to complete it will return <code>true</code> indicating
         * that any new row added the output can terminate via an
         * {@link org.knime.core.node.streamable.RowOutput.OutputClosedException}.
         * @param rowCache The output cache that has been closed.
         * @return <code>true</code> if all output caches are closed */
        public synchronized boolean closeOutput(final InMemoryRowCache rowCache) {
            m_closedCachesSet.add(rowCache);
            final int outputsClosedCount = m_closedCachesSet.size();
            CheckUtils.checkState(outputsClosedCount <= m_totalOutputCount, "Can't close more outputs than available");
            return outputsClosedCount == m_totalOutputCount;
        }


    }

}
