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

import java.util.concurrent.Callable;
import java.util.stream.Stream;

import org.knime.core.node.ExecutionContext;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.streamable.PortInput;
import org.knime.core.node.streamable.PortOutput;
import org.knime.core.node.streamable.RowInput;
import org.knime.core.node.streamable.RowOutput;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.workflow.ConnectionContainer;
import org.knime.core.node.workflow.FlowObjectStack;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.node.workflow.NodeExecutionJob;
import org.knime.core.node.workflow.SingleNodeContainer;
import org.knime.core.streaming.inoutput.AbstractOutputCache;

/**
 * The creator of new {@link Callable} to process a node's execution. So far each node is run by a single thread.
 *
 * @author Bernd Wiswedel, KNIME AG, Zurich, Switzerland
 */
final class SingleNodeStreamer {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SingleNodeStreamer.class);

    private final SingleNodeContainer m_snc;
    private final AbstractOutputCache[] m_outputCaches;
    private final AbstractOutputCache[] m_upStreamCaches;
    private final ExecutionContext m_execContext;

    /**
     * @param snc The node to execute, not null.
     * @param execContext TODO
     * @param outputCaches TODO
     */
    SingleNodeStreamer(final SingleNodeContainer snc, final ExecutionContext execContext,
        final AbstractOutputCache[] outputCaches, final AbstractOutputCache[] upStreamCaches) {
        m_execContext = CheckUtils.checkArgumentNotNull(execContext, "Arg must not be null");
        m_snc = CheckUtils.checkArgumentNotNull(snc, "Arg must not be null");
        m_outputCaches = CheckUtils.checkArgumentNotNull(outputCaches, "Arg must not be null");
        m_upStreamCaches = CheckUtils.checkArgumentNotNull(upStreamCaches, "Arg must not be null");
    }

    NodeExecutionJob submitJob() {
        updateThreadName(m_snc.getNameWithID());
        NodeContext.pushContext(m_snc);
        final PortInput[] inputs = new PortInput[m_upStreamCaches.length];
        final PortOutput[] outputs = new PortOutput[m_outputCaches.length];
        try {
            for (int i = 0; i < m_upStreamCaches.length; i++) {
                final ConnectionContainer inCC = m_snc.getParent().getIncomingConnectionFor(m_snc.getID(), i);
                inputs[i] = m_upStreamCaches[i].getPortInput(m_snc.getInputPortRoles()[i], inCC);
                final FlowObjectStack stack = m_upStreamCaches[i].getFlowObjectStack(m_snc.getInputPortRoles()[i]);
            }

            final PortObjectSpec[] inSpecs = new PortObjectSpec[m_upStreamCaches.length];
            for (int i = 0; i < m_upStreamCaches.length; i++) {
                inSpecs[i] = m_upStreamCaches[i].getPortObjectSpec();
            }
            return m_snc.findJobManager().submitJob(m_snc, inputs, outputs);
        } catch (InterruptedException e) {
            // downstream error - ignore here
            LOGGER.debugWithFormat("interrupt received for \"%s\"- canceling streaming execution",
                m_snc.getNameWithID());
            return null;
        } finally {
            NodeContext.removeLastContext();
            updateThreadName("IDLE");
        }
    }

    static void updateThreadName(final String nameSuffix) {
        String name = Thread.currentThread().getName();
        name = name.replaceAll("^(Streaming-\\d+).*", "$1-" + nameSuffix);
        Thread.currentThread().setName(name);
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