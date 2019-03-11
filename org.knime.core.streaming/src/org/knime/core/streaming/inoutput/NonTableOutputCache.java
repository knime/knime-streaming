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

import java.io.IOException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.Node;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.streamable.InputPortRole;
import org.knime.core.node.streamable.PortInput;
import org.knime.core.node.streamable.PortObjectInput;
import org.knime.core.node.streamable.PortObjectOutput;
import org.knime.core.node.streamable.PortOutput;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.workflow.ConnectionContainer;
import org.knime.core.node.workflow.FlowObjectStack;
import org.knime.core.node.workflow.SingleNodeContainer;

/** An output cache used for all non-table ports. It will block all input retrieval until the data is pushed by
 * the producing node.
 * @author Bernd Wiswedel, KNIME AG, Zurich, Switzerland
 */
public final class NonTableOutputCache extends AbstractOutputCache<PortObjectSpec> {

    private final Condition m_portObjectInputNotSetCondition;
    private final PortObjectOutput m_portObjectOutput;
    private PortObject m_portObject;

    /** Only inits fields and sets spec in super class.
     * @param nnc The associated node (used to query variables). */
    public NonTableOutputCache(final SingleNodeContainer nnc) {
        super(CheckUtils.checkArgumentNotNull(nnc), PortObjectSpec.class);
        m_portObjectInputNotSetCondition = getLock().newCondition();
        m_portObjectOutput = new SyncedPortObjectOutput();
    }

    /** Sets the port object (and spec) and wakes up threads waiting for it.
     * @param portObject Non-null port object
     * @throws IllegalStateException If an object was set already.
     */
    public void setObject(final PortObject portObject) {
        final Lock lock = getLock();
        lock.lock();
        try {
            CheckUtils.checkState(m_portObject == null, "Port object has been set previously (not null) \"%s\" vs. "
                + "\"%s\" - objects are%s identical", m_portObject, portObject,
                (m_portObject == portObject ? "": " not"));
            m_portObject = CheckUtils.checkArgumentNotNull(portObject, "Port Object must not be null");
            setPortObjectSpec(getSpecClass().cast(portObject.getSpec()));
            m_portObjectInputNotSetCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** Get the port object, waiting for it if not available.
     * @return the non-null object.
     * @throws InterruptedException When interrupted while waiting for it to be set.
     */
    PortObject getPortObject() throws InterruptedException {
        final Lock lock = getLock();
        lock.lockInterruptibly();
        try {
            while (m_portObject == null) {
                m_portObjectInputNotSetCondition.await();
            }
            return m_portObject;
        } finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public PortInput getPortInput(final InputPortRole role, final ConnectionContainer cc, final ExecutionContext exec)
        throws InterruptedException, IOException, CanceledExecutionException {
        CheckUtils.checkState(!role.isStreamable(), "Non-table port can't be streamed");
        CheckUtils.checkState(!role.isDistributable(), "Non-table port can't be distributed");
        return new PortObjectInput(Node.copyPortObject(getPortObject(), exec));
    }

    /** {@inheritDoc} */
    @Override
    public PortOutput getPortOutput() {
        return m_portObjectOutput;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PortObject getPortObjectMock() {
        CheckUtils.checkState(m_portObject != null, "PortObject expected to be set at this point");
        return m_portObject;
    }

    /** {@inheritDoc} */
    @Override
    public FlowObjectStack getFlowObjectStack(final InputPortRole inputRole) throws InterruptedException {
        CheckUtils.checkState(!inputRole.isStreamable(), "Non-table port can't be streamed");
        final ReentrantLock lock = getLock();
        lock.lockInterruptibly();
        try {
            getPortObject(); // wait for output to be populated
            return getSingleNodeContainer().createOutFlowObjectStack();
        } finally {
            lock.unlock();
        }
    }

    /** Output object that redirects the object into this cache. */
    final class SyncedPortObjectOutput extends PortObjectOutput {

        /** {@inheritDoc} */
        @Override
        public void setPortObject(final PortObject portObject) {
            super.setPortObject(portObject);
            setObject(portObject);
        }
    }

}
