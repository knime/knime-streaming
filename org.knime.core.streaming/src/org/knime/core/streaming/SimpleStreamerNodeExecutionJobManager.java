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

import java.net.URL;

import org.knime.core.node.NodeModel;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.streamable.PartitionInfo;
import org.knime.core.node.workflow.AbstractNodeExecutionJobManager;
import org.knime.core.node.workflow.NativeNodeContainer;
import org.knime.core.node.workflow.NodeContainer;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.node.workflow.NodeExecutionJob;

/**
 * Job Manager for a streaming sub node.
 * @author Bernd Wiswedel, KNIME.com, Zurich, Switzerland
 * @author Michael Berthold, KNIME.com, Zurich, Switzerland
 */
public class SimpleStreamerNodeExecutionJobManager extends AbstractNodeExecutionJobManager {

    private static final URL STREAMING_GREEN =
            SimpleStreamerNodeExecutionJobManager.class.getResource("icons/streaming_green.png");

    private static final URL STREAMING_RED =
            SimpleStreamerNodeExecutionJobManager.class.getResource("icons/streaming_red.png");

    /** {@inheritDoc} */
    @Override
    public NodeExecutionJob submitJob(final NodeContainer nc, final PortObject[] data) {
        final SimpleStreamerNodeExecutionJob job = new SimpleStreamerNodeExecutionJob(nc, data);
        SimpleStreamerNodeExecutionJob.STREAMING_EXECUTOR_SERVICE.execute(new Runnable() {
            @Override
            public void run() {
                SingleNodeStreamer.updateThreadName("Master");
                NodeContext.pushContext(nc);
                try {
                    job.run();
                } finally {
                    NodeContext.removeLastContext();
                    SingleNodeStreamer.updateThreadName("IDLE");
                }
            }
        });
        return job;
    }

    /** {@inheritDoc} */
    @Override
    public String getID() {
        return SimpleStreamerNodeExecutionJobManagerFactory.ID;
    }

    /** {@inheritDoc} */
    @Override
    public URL getIcon() {
        return getClass().getResource("icons/streaming.png");
    }

    /** {@inheritDoc} */
    @Override
    // Determines the decorating icon for a native node. Green = natively streams; Red = not streaming.
    public URL getIconForChild(final NodeContainer child) {
        return implementsStreamingAPI(child) ? STREAMING_GREEN : STREAMING_RED;
    }

    /** Uses reflection to test whether the underlying NodeModel implements any of
     * {@link NodeModel#getInputPortRoles()}, {@link NodeModel#getOutputPortRoles()}, or
     * {@link NodeModel#createStreamableOperator(PartitionInfo, PortObjectSpec[])}
     *
     * <p />
     * If so a different icon is shown attached to the node icon or a different action is used within the executor.
     * Meta nodes and Sub nodes are not streamable (but their content may).
     * @param nc The node to test.
     * @return That property.
     */
    public static final boolean implementsStreamingAPI(final NodeContainer nc) {
        if (nc instanceof NativeNodeContainer ) {
            NativeNodeContainer nnc = (NativeNodeContainer)nc;
            NodeModel model = nnc.getNodeModel();
            Class<?> cl = model.getClass();
            do {
                try {
                    cl.getDeclaredMethod("getInputPortRoles");
                    return true;
                } catch (Exception e) {
                    // ignore, check superclass
                }
                try {
                    cl.getDeclaredMethod("getOutputPortRoles");
                    return true;
                } catch (Exception e) {
                    // ignore, check superclass
                }
                try {
                    cl.getDeclaredMethod("createStreamableOperator", PartitionInfo.class, PortObjectSpec[].class);
                    return true;
                } catch (Exception e) {
                    // ignore, check superclass
                }
                cl = cl.getSuperclass();
            } while (!NodeModel.class.equals(cl));
            return false;
        } else {
            return false;
        }
    }

    /** {@inheritDoc} */
    @Override
    public URL getIconForWorkflow() {
        return getClass().getResource("icons/streaming_flow.png");
    }

}