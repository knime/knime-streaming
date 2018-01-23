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

import java.lang.Thread.UncaughtExceptionHandler;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.streamable.PartitionInfo;
import org.knime.core.node.workflow.AbstractNodeExecutionJobManager;
import org.knime.core.node.workflow.NativeNodeContainer;
import org.knime.core.node.workflow.NodeContainer;
import org.knime.core.node.workflow.NodeContainer.NodeContainerSettings.SplitType;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.node.workflow.NodeExecutionJob;
import org.knime.core.node.workflow.NodeExecutionJobManagerPanel;
import org.knime.core.node.workflow.SubNodeContainer;

/**
 * Job Manager for a streaming sub node.
 * @author Bernd Wiswedel, KNIME AG, Zurich, Switzerland
 * @author Michael Berthold, KNIME AG, Zurich, Switzerland
 */
public class SimpleStreamerNodeExecutionJobManager extends AbstractNodeExecutionJobManager {

    private static final URL STREAMING_GREEN =
            SimpleStreamerNodeExecutionJobManager.class.getResource("icons/streaming_green.png");

    private static final URL STREAMING_RED =
            SimpleStreamerNodeExecutionJobManager.class.getResource("icons/streaming_red.png");

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SimpleStreamerNodeExecutionJobManager.class);

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

    private SimpleStreamerNodeExecutionSettings m_settings = new SimpleStreamerNodeExecutionSettings();

    /** {@inheritDoc} */
    @Override
    public NodeExecutionJob submitJob(final NodeContainer nc, final PortObject[] data) {
        final SimpleStreamerNodeExecutionJob job = new SimpleStreamerNodeExecutionJob(nc, data, m_settings);
        STREAMING_EXECUTOR_SERVICE.execute(new Runnable() {
            @Override
            public void run() {
//                SingleNodeStreamer.updateThreadName("Master");
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

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeExecutionJobManagerPanel getSettingsPanelComponent(final SplitType nodeSplitType) {
        return new SimpleStreamerNodeExecutionJobManagerPanel();
    }

    /** {@inheritDoc} */
    @Override
    public void load(final NodeSettingsRO settings) throws InvalidSettingsException {
        SimpleStreamerNodeExecutionSettings c = new SimpleStreamerNodeExecutionSettings();
        c.loadSettingsInModel(settings);
        m_settings = c;
    }

    /** {@inheritDoc} */
    @Override
    public void save(final NodeSettingsWO settings) {
        m_settings.saveSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean canExecute(final NodeContainer nc) {
        return nc instanceof SubNodeContainer;
    }

}