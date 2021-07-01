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
 *   Mar 25, 2015 (wiswedel): created
 */
package org.knime.core.streaming;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.lang3.ArrayUtils;
import org.knime.core.data.DataRow;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.flowvariable.FlowVariablePortObject;
import org.knime.core.node.port.flowvariable.FlowVariablePortObjectSpec;
import org.knime.core.node.port.inactive.InactiveBranchConsumer;
import org.knime.core.node.port.inactive.InactiveBranchPortObjectSpec;
import org.knime.core.node.streamable.DataTableRowInput;
import org.knime.core.node.streamable.InputPortRole;
import org.knime.core.node.streamable.MergeOperator;
import org.knime.core.node.streamable.OutputPortRole;
import org.knime.core.node.streamable.PartitionInfo;
import org.knime.core.node.streamable.PortInput;
import org.knime.core.node.streamable.PortOutput;
import org.knime.core.node.streamable.RowInput;
import org.knime.core.node.streamable.RowOutput;
import org.knime.core.node.streamable.RowOutput.OutputClosedException;
import org.knime.core.node.streamable.StreamableOperator;
import org.knime.core.node.streamable.StreamableOperatorInternals;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.workflow.ConnectionContainer;
import org.knime.core.node.workflow.FlowObjectStack;
import org.knime.core.node.workflow.NativeNodeContainer;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.node.workflow.NodeMessage;
import org.knime.core.node.workflow.WorkflowLock;
import org.knime.core.node.workflow.WorkflowManager;
import org.knime.core.node.workflow.execresult.NativeNodeContainerExecutionResult;
import org.knime.core.node.workflow.execresult.NodeExecutionResult;
import org.knime.core.streaming.inoutput.AbstractOutputCache;
import org.knime.core.streaming.inoutput.NonTableOutputCache;
import org.knime.core.streaming.inoutput.StagedTableRowInput;

/**
 * The creator of new {@link Callable} to process a node's execution. So far each node is run by a single thread.
 *
 * @author Bernd Wiswedel, KNIME AG, Zurich, Switzerland
 */
final class SingleNodeStreamer {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SingleNodeStreamer.class);

    private final NativeNodeContainer m_nnc;
    /** Buffers data produced at the output ports */
    private final AbstractOutputCache[] m_outputCaches;
    private final AbstractOutputCache[] m_upStreamCaches;
    private final ExecutionContext m_execContext;

    /**
     * @param nnc The node to execute, not null.
     * @param execContext TODO
     * @param outputCaches TODO
     */
    SingleNodeStreamer(final NativeNodeContainer nnc, final ExecutionContext execContext,
        final AbstractOutputCache[] outputCaches, final AbstractOutputCache[] upStreamCaches) {
        m_execContext = CheckUtils.checkArgumentNotNull(execContext, "Arg must not be null");
        m_nnc = CheckUtils.checkArgumentNotNull(nnc, "Arg must not be null");
        m_outputCaches = CheckUtils.checkArgumentNotNull(outputCaches, "Arg must not be null");
        m_upStreamCaches = CheckUtils.checkArgumentNotNull(upStreamCaches, "Arg must not be null");
        m_nnc.getNode().openFileStoreHandler(m_execContext);
    }

    Callable<NativeNodeContainerExecutionResult> newCallable() {
        return new SingleNodeStreamerCallable();
    }

    static String getThreadName(final String nameSuffix) {
        return Thread.currentThread().getName().replaceAll("^(Streaming-\\d+).*", "$1-" + nameSuffix);
    }

    final class SingleNodeStreamerCallable implements Callable<NativeNodeContainerExecutionResult> {

        /**
         * 1. prepare input buffers. If one of them is inactive and this node does not consume inactive branches: set
         * all downstream buffers to inactive and return an empty execution result.
         *
         * 2. get upstream port object specs, flow variables, and handles for data
         *
         * 3. perform the streaming iteration step (StreamableOperator#runIntermediate and
         * MergeOperator#mergeIntermediate)
         *
         * 4. propagate flow variables, configure node, and compute final output specs
         *
         * 5. prepare output buffers
         *
         * 6. execute final (as opposed to intermediate) streaming step
         */
        @Override
        public NativeNodeContainerExecutionResult call() {
            Thread.currentThread().setName(getThreadName(m_nnc.getNameWithID()));
            NodeContext.pushContext(m_nnc);
            final PortInput[] inputs = new PortInput[m_upStreamCaches.length];
            final PortOutput[] outputs = new PortOutput[m_outputCaches.length];
            try {
                final WorkflowManager parent = m_nnc.getParent();
                final NodeModel nM = m_nnc.getNodeModel();
                final InputPortRole[] inputPortRoles = ArrayUtils.add(
                    nM.getInputPortRoles(), 0, InputPortRole.NONDISTRIBUTED_NONSTREAMABLE);

                //
                // 1. check and prepare input buffers, abort if this node gets but can't handle inactive branches
                //
                boolean isInactive = false;
                for (int i = 0; i < m_upStreamCaches.length; i++) {
                    m_upStreamCaches[i].prepare();
                    if (m_upStreamCaches[i].isInactive()) {
                        isInactive = true;
                    }
                }
                if (isInactive) {
                    if (!(nM instanceof InactiveBranchConsumer)) {
                        Arrays.stream(m_outputCaches).forEach(c -> c.setInactive());

                        //early abort
                        NativeNodeContainerExecutionResult executionResult =
                            m_nnc.createExecutionResult(m_execContext.createSubProgress(0.0));
                        executionResult.setSuccess(true);
                        return executionResult;
                    }
                }

                //
                // 2. get upstream port object specs, flow variables, and handles for data
                //
                final FlowObjectStack[] flowObjectStacks = new FlowObjectStack[m_upStreamCaches.length];
                for (int i = 0; i < m_upStreamCaches.length; i++) {
                    final ConnectionContainer inCC = parent.getIncomingConnectionFor(m_nnc.getID(), i);
                    inputs[i] = m_upStreamCaches[i].getPortInput(inputPortRoles[i], inCC, m_execContext);
                    final FlowObjectStack stack = m_upStreamCaches[i].getFlowObjectStack(inputPortRoles[i]);
                    flowObjectStacks[i] = stack;
                }
                // get specs
                // IMPORTANT NOTE: 'getPortInput' needs be called before 'getPortObjectSpec'
                // otherwise, e.g., the column domains might not be properly set in the spec(s)
                final PortObjectSpec[] inSpecs = new PortObjectSpec[m_upStreamCaches.length];
                for (int i = 0; i < m_upStreamCaches.length; i++) {
                    if (m_upStreamCaches[i].isInactive()) {
                        inSpecs[i] = InactiveBranchPortObjectSpec.INSTANCE;
                    } else {
                        inSpecs[i] = m_upStreamCaches[i].getPortObjectSpec();
                    }
                }
                final PortObjectSpec[] inSpecsNoFlowPort = ArrayUtils.remove(inSpecs, 0);

                //
                // 3. do iterations (StreamableOperator#runIntermediate)
                //
                // can be null
                MergeOperator mergeOperator = nM.createMergeOperator();

                // never be null
                final StreamableOperator strop =
                    nM.createStreamableOperator(new PartitionInfo(0, 1), inSpecsNoFlowPort);

                // pre-iterate if necessary
                // this mutates inputs if iterations are requested: the input data is copied to data tables
                StreamableOperatorInternals streamInternals =
                    iterateExecution(nM, inputPortRoles, inputs, mergeOperator, strop);

                //
                // 4. propagate flow variables, configure node, and compute final output specs
                //
                boolean isConfigureOK;

                try (WorkflowLock lock = m_nnc.getParent().lock()) {
                    m_nnc.getParent().createAndSetFlowObjectStackFor(m_nnc, flowObjectStacks);
                    // need a final call to configure in order to propagate flow variables into the configuration
                    //
                    // this potentially has race conditions with streamed inports - it's not deterministic if an
                    // upstream node has published all flow variables when this method gets call. It's deterministic
                    // for non-streamable ports as their port objects are already queried/available (unless a node
                    // pushes flow variables after setting the result)
                    isConfigureOK = m_nnc.callNodeConfigure(inSpecs, true);
                }

                CheckUtils.checkSetting(isConfigureOK, "Configuration failed");

                PortObjectSpec[] outSpecsNoFlowPort = nM.computeFinalOutputSpecs(streamInternals, inSpecsNoFlowPort);

                //
                // 5. prepare output buffers
                //
                if (outSpecsNoFlowPort != null) {
                    m_outputCaches[0].setPortObjectSpec(FlowVariablePortObjectSpec.INSTANCE);
                    CheckUtils.checkState(outSpecsNoFlowPort.length == m_nnc.getNrOutPorts() - 1, "Wrong number of "
                        + "output ports: got %d, expected %d", outSpecsNoFlowPort.length, m_nnc.getNrOutPorts() - 1);
                    for (int i = 0; i < outSpecsNoFlowPort.length; i++) {
                        final PortObjectSpec s = outSpecsNoFlowPort[i];
                        if (s != null) {
                            // AP-16977: setPortObjectSpec can't handle InactiveBranchPortObjectSpec
                            if (s instanceof InactiveBranchPortObjectSpec) {
                                m_outputCaches[i + 1].setInactive();
                            } else {
                                m_outputCaches[i + 1].setPortObjectSpec(s);
                            }
                        }
                    }
                }

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


                //
                // 6. execute final (as opposed to intermediate) streaming step
                //
                strop.loadInternals(streamInternals);
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
                m_nnc.getNode().assignInternalHeldObjects(rawInput, null, m_execContext, rawOutput);
                m_execContext.setMessage("Creating execution result");
                NativeNodeContainerExecutionResult executionResult =
                        m_nnc.createExecutionResult(m_execContext.createSubProgress(0.0));
                executionResult.setSuccess(true);
                return executionResult;
            } catch (Exception e) {
                NativeNodeContainerExecutionResult r = new NativeNodeContainerExecutionResult();
                r.setNodeExecutionResult(new NodeExecutionResult());
                if (e instanceof InterruptedException) {
                    // downstream error - ignore here
                    LOGGER.debugWithFormat("interrupt received for \"%s\"- canceling streaming execution",
                        m_nnc.getNameWithID());
                } else if (e instanceof InvalidSettingsException) {
                    LOGGER.warn(e.getMessage(), e);
                    r.setMessage(NodeMessage.newWarning(e.getMessage()));
                } else {
                    LOGGER.error(e.getMessage(), e);
                    r.setMessage(NodeMessage.newError(e.getMessage()));
                }
                r.setSuccess(false);
                return r;
            } finally {
                NodeContext.removeLastContext();
                Thread.currentThread().setName(getThreadName("IDLE"));
            }
        }

        /**
         * Repeatedly execute the given {@link StreamableOperator}. Since we need the output of the upstream nodes again
         * for each iteration, the output is copied ("staging") to {@link BufferedDataTable}s pior to the first
         * iteration. For subsequent iterations, the {@link PortInput} objects are created again using the staged
         * tables. {@link PortInput}s to tables
         *
         * @param nM
         * @param inputPortRoles
         * @param inputs read+write: creates inputs again if depleted after an iteration
         * @param mergeOperator if non-null, executed at the end of each iteration to replace the stream internals
         *            output by strop in that iteration
         * @param strop read+write: loads the internals, executes, and saves the internals in each iteration
         * @return
         * @throws InterruptedException
         * @throws CanceledExecutionException
         * @throws Exception
         */
        private StreamableOperatorInternals iterateExecution(final NodeModel nM, final InputPortRole[] inputPortRoles,
            final PortInput[] inputs, final MergeOperator mergeOperator, final StreamableOperator strop)
            throws InterruptedException, CanceledExecutionException, Exception {

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

                // (re-)create streamable staged inputs: streams are now depleted
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
            return streamInternals;
        }

    }

    /** Called after processing to close all outputs.
     * @param outputs to be closed, non null.
     */
    private static void closeOutputs(final PortOutput[] outputs) throws InterruptedException {
        for (int i = 0; i < outputs.length; i++) {
            if (outputs[i] != null && outputs[i] instanceof RowOutput) {
                ((RowOutput)outputs[i]).close();
            }
        }
    }

    /** Called after processing to close all inputs
     * @param inputs to be closed, non-null
     */
    private static void closeInputs(final PortInput[] inputs) {
        Stream.of(inputs).filter(i -> i instanceof RowInput).map(i -> (RowInput)i).forEach(i -> i.close());
    }
}