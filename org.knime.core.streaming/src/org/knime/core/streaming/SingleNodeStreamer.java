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
 *   Mar 25, 2015 (wiswedel): created
 */
package org.knime.core.streaming;

import java.util.concurrent.Callable;
import java.util.stream.Stream;

import org.apache.commons.lang3.ArrayUtils;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.flowvariable.FlowVariablePortObject;
import org.knime.core.node.port.flowvariable.FlowVariablePortObjectSpec;
import org.knime.core.node.streamable.InputPortRole;
import org.knime.core.node.streamable.PartitionInfo;
import org.knime.core.node.streamable.PortInput;
import org.knime.core.node.streamable.PortOutput;
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

/**
 * The creator of new {@link Callable} to process a node's execution. So far each node is run by a single thread.
 *
 * @author Bernd Wiswedel, KNIME.com, Zurich, Switzerland
 */
final class SingleNodeStreamer {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SingleNodeStreamer.class);

    private final NativeNodeContainer m_nnc;
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
    }

    Callable<NativeNodeContainerExecutionResult> newCallable() {
        return new SingleNodeStreamerCallable();
    }

    static void updateThreadName(final String nameSuffix) {
        String name = Thread.currentThread().getName();
        name = name.replaceAll("^(Streaming-\\d+).*", "$1-" + nameSuffix);
        Thread.currentThread().setName(name);
    }

    final class SingleNodeStreamerCallable implements Callable<NativeNodeContainerExecutionResult> {

        @Override
        public NativeNodeContainerExecutionResult call() {
            updateThreadName(m_nnc.getNameWithID());
            NodeContext.pushContext(m_nnc);
            final PortInput[] inputs = new PortInput[m_upStreamCaches.length];
            final PortOutput[] outputs = new PortOutput[m_outputCaches.length];
            try {
                final WorkflowManager parent = m_nnc.getParent();
                final NodeModel nM = m_nnc.getNodeModel();
                final InputPortRole[] inputPortRoles = ArrayUtils.add(
                    nM.getInputPortRoles(), 0, InputPortRole.NONDISTRIBUTED_NONSTREAMABLE);

                final FlowObjectStack[] flowObjectStacks = new FlowObjectStack[m_upStreamCaches.length];
                for (int i = 0; i < m_upStreamCaches.length; i++) {
                    final ConnectionContainer inCC = parent.getIncomingConnectionFor(m_nnc.getID(), i);
                    inputs[i] = m_upStreamCaches[i].getPortInput(inputPortRoles[i], inCC);
                    final FlowObjectStack stack = m_upStreamCaches[i].getFlowObjectStack(inputPortRoles[i]);
                    flowObjectStacks[i] = stack;
                }

                StreamableOperatorInternals streamInternals = nM.createInitialStreamableOperatorInternals();
                if (nM.iterate(streamInternals)) {
                    throw new Exception("Streaming iteration not implemented");
                }

                final PortObjectSpec[] inSpecs = new PortObjectSpec[m_upStreamCaches.length];
                for (int i = 0; i < m_upStreamCaches.length; i++) {
                    inSpecs[i] = m_upStreamCaches[i].getPortObjectSpec();
                }
                final PortObjectSpec[] inSpecsNoFlowPort = ArrayUtils.remove(inSpecs, 0);

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
                if (outSpecsNoFlowPort != null) {
                    m_outputCaches[0].setPortObjectSpec(FlowVariablePortObjectSpec.INSTANCE);
                    CheckUtils.checkState(outSpecsNoFlowPort.length == m_nnc.getNrOutPorts() - 1, "Wrong number of "
                        + "output ports: got %d, expected %d", outSpecsNoFlowPort.length, m_nnc.getNrOutPorts() - 1);
                    for (int i = 0; i < outSpecsNoFlowPort.length; i++) {
                        final PortObjectSpec s = outSpecsNoFlowPort[i];
                        if (s != null) {
                            m_outputCaches[i + 1].setPortObjectSpec(s);
                        }
                    }
                }

                for (int i = 0; i < outputs.length; i++) {
                    outputs[i] = m_outputCaches[i].getPortOutput();
                }

                final StreamableOperator strop = nM.createStreamableOperator(new PartitionInfo(0, 1), inSpecsNoFlowPort);
                m_nnc.getNode().openFileStoreHandler(m_execContext);
                strop.runFinal(ArrayUtils.remove(inputs, 0), ArrayUtils.remove(outputs, 0),
                    m_execContext.createSubExecutionContext(0.0));
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
                    String error = "(" + e.getClass().getSimpleName() + "): " + e.getMessage();
                    LOGGER.error(error, e);
                    r.setMessage(NodeMessage.newError(e.getMessage()));
                }
                r.setSuccess(false);
                return r;
            } finally {
                NodeContext.removeLastContext();
                updateThreadName("IDLE");
            }
        }

    }
}