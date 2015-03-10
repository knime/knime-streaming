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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Set;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.streamable.PartitionInfo;
import org.knime.core.node.streamable.StreamableOperator;
import org.knime.core.node.workflow.ConnectionContainer;
import org.knime.core.node.workflow.NativeNodeContainer;
import org.knime.core.node.workflow.NodeContainer;
import org.knime.core.node.workflow.NodeExecutionJob;
import org.knime.core.node.workflow.NodeID;
import org.knime.core.node.workflow.SubNodeContainer;
import org.knime.core.node.workflow.WorkflowManager;
import org.knime.core.node.workflow.execresult.NodeContainerExecutionStatus;
import org.knime.core.streaming.inoutput.InMemoryRowCache;
import org.knime.core.streaming.inoutput.InMemoryRowInput;
import org.knime.core.streaming.inoutput.InMemoryRowOutput;
import org.knime.core.util.Pair;
import org.knime.core.util.ThreadUtils;

/**
 *
 * @author Bernd Wiswedel, KNIME.com, Zurich, Switzerland
 */
public final class SimpleStreamerNodeExecutionJob extends NodeExecutionJob {

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
        NodeContainer nodeContainer = getNodeContainer();
        if (!(nodeContainer instanceof SubNodeContainer)) {
            return NodeContainerExecutionStatus.FAILURE;
        }
        WorkflowManager wfm = ((SubNodeContainer)nodeContainer).getWorkflowManager();
        HashMap<Pair<NodeID, Integer>, InMemoryRowCache> connectionCaches
            = new LinkedHashMap<Pair<NodeID, Integer>, InMemoryRowCache>();
        for (NodeContainer nc : wfm.getNodeContainers()) {
            if (!(nodeContainer instanceof NativeNodeContainer)) {
                return NodeContainerExecutionStatus.FAILURE;
            }
            for (int op = 0; op < nc.getNrOutPorts(); op++) {
                Set<ConnectionContainer> ccs = wfm.getOutgoingConnectionsFor(nc.getID(), op);
                DataTableSpec spec = (DataTableSpec)(nc.getOutPort(op).getPortObjectSpec());
                connectionCaches.put(new Pair<NodeID, Integer>(nc.getID(), op), new InMemoryRowCache(ccs.size(), spec));
            }
        }
        for (NodeContainer nc : wfm.getNodeContainers()) {
            final NativeNodeContainer nnc = (NativeNodeContainer)nc;
            // collect incoming caches
            final InMemoryRowInput[]  inCaches = new InMemoryRowInput[nnc.getNrInPorts()];
            for (int i = 0; i < inCaches.length; i++) {
                ConnectionContainer cc = wfm.getIncomingConnectionFor(nnc.getID(), i);
                if (cc == null) {
                    return NodeContainerExecutionStatus.FAILURE;
                }
                InMemoryRowCache imrc = connectionCaches.get(new Pair<NodeID, Integer>(cc.getSource(), cc.getSourcePort()));
                inCaches[i] = imrc.createRowInput();
            }
            // collect outgoing caches
            final InMemoryRowOutput[]  outCaches = new InMemoryRowOutput[nnc.getNrOutPorts()];
            for (int o = 0; o < outCaches.length; o++) {
                InMemoryRowCache imrc = connectionCaches.get(new Pair<NodeID, Integer>(nnc.getID(), o));
                outCaches[o] = imrc.createRowOutput();
            }
            // initiate actual work
            try {
                PortObjectSpec specs[] = new PortObjectSpec[nnc.getNrOutPorts()];
                for (int o = 0; o < nnc.getNrOutPorts(); o++) {
                    specs[o] = nnc.getOutPort(o).getPortObjectSpec();
                }
                final StreamableOperator strop = nnc.getNodeModel().createStreamableOperator(
                    new PartitionInfo(0, 1), specs);
                ThreadUtils.threadWithContext(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            strop.runFinal(inCaches, outCaches, nnc.createExecutionContext());
                        } catch (Exception e) {};
                    }
                }, "Mein Thread").start();
            } catch (Exception e) {};        }
        return null;
    }

    /** {@inheritDoc} */
    @Override
    protected boolean cancel() {
        return false;
    }

}
