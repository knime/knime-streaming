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
package org.knime.core.streaming.inoutput;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.streamable.InputPortRole;
import org.knime.core.node.streamable.PortInput;
import org.knime.core.node.streamable.PortOutput;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.workflow.ConnectionContainer;
import org.knime.core.node.workflow.FlowObjectStack;
import org.knime.core.node.workflow.SingleNodeContainer;

/**
 * Abstract object associated with a port output that synchronizes access to the port resource. A cache gets filled by a
 * producer node (upstream) and is consumed by multiple (0-n) consumer nodes (down-stream).
 *
 * @param <SPEC> The class of port object spec associated with the output.
 * @author Bernd Wiswedel, KNIME AG, Zurich, Switzerland
 */
public abstract class AbstractOutputCache<SPEC extends PortObjectSpec> {

    private final ReentrantLock m_lock;
    private final Condition m_portObjectSpecNotSetCondition;

    private final SingleNodeContainer m_snc;

    private final Class<SPEC> m_specClass;

    private SPEC m_portObjectSpec;

    /** Inits locks etc.
     * @param nnc The associated node (used to query variables).
     * @param cl Class of the spec. */
    AbstractOutputCache(final SingleNodeContainer nnc, final Class<SPEC> cl) {
        m_lock = new ReentrantLock();
        m_snc = nnc;
        m_portObjectSpecNotSetCondition = m_lock.newCondition();
        m_specClass = CheckUtils.checkArgumentNotNull(cl, "Arg must not be null");
    }

    /** The non-null final lock used to sychronize any access.
     * @return the lock */
    final ReentrantLock getLock() {
        return m_lock;
    }

    /** @return the specClass associated with the output. */
    final Class<SPEC> getSpecClass() {
        return m_specClass;
    }

    /**
     * Finally sets the port object spec. This is called by the producing node. This will unlock waiting threads and
     * allow downstream nodes to continue (e.g. compute their final spec and then start the computation ... which then
     * again stalls until data is available).
     *
     * @param spec The non-null spec for this output.
     */
    public void setPortObjectSpec(final SPEC spec) {
        m_lock.lock();
        try {
            m_portObjectSpec = CheckUtils.checkArgumentNotNull(spec, "Argument must not be null");
            m_portObjectSpecNotSetCondition.signalAll();
        } finally {
            m_lock.unlock();
        }
    }

    /**
     * Gets the output spec associated with the output (stalling until one is available).
     *
     * @return The non-null spec.
     * @throws InterruptedException when interrupted while waiting for the spec to be set.
     */
    public SPEC getPortObjectSpec() throws InterruptedException {
        m_lock.lockInterruptibly();
        try {
            while (m_portObjectSpec == null) {
                m_portObjectSpecNotSetCondition.await();
            }
            return m_portObjectSpec;
        } finally {
            m_lock.unlock();
        }
    }

    /** Plain getter to port object spec - returning null if none has been set.
     * @return The spec or null. */
    public SPEC getPortObjectSpecNoWait() {
        return m_portObjectSpec;
    }

    /** @return the node as set in constructor. */
    final SingleNodeContainer getSingleNodeContainer() {
        return m_snc;
    }

    /** The outgoing flow object stack associated with the node (may be an empty stack for streamed connections).
     * @param inputRole role of the input of the downstream node - only non-streamable will receive flow objects
     * @return the upstream flow object stack
     * @throws InterruptedException while canceled while waiting for completion.
     */
    public abstract FlowObjectStack getFlowObjectStack(final InputPortRole inputRole) throws InterruptedException;

    /**
     * Call by the outport's node to have a handle to push the output to. This method is supposed to be called only once
     * (or at least return always the same object).
     *
     * @return The non-null output that then evenually will put spec and data into this cache.
     */
    public abstract PortOutput getPortOutput();

    /** A portobject that is put into the node's output after the streamer finishes execution (all green). For
     * non-table ports this usually contains a real object (e.g. PMML); for table ports this is usually a void table.
     * @return port object to be put into node's output, not null.
     */
    public abstract PortObject getPortObjectMock();

    /**
     * Called by downstream workers to get a handle to the data. This method will block until all set-up is done, e.g.
     * for ordinary port objects it will block until the object itself is available. For streamed data ports this will
     * block until at least the spec of the table is available -- the data passing itself is then synchronized with the
     * returned PortInput.
     *
     * @param role The role of the connection, e.g. for data ports if it can be streamed or needs staging.
     * @param cc The connection itself (for progress events).
     * @return The non-null port input.
     * @throws InterruptedException If canceled while waiting for the set-up.
     */
    public abstract PortInput getPortInput(final InputPortRole role, final ConnectionContainer cc)
        throws InterruptedException;

}
