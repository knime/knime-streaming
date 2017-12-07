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
 *   Dec 6, 2017 (clemens): created
 */
package org.knime.core.streaming.inoutput;

import java.io.Serializable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.streamable.InputPortRole;
import org.knime.core.node.streamable.PortInput;
import org.knime.core.node.streamable.PortOutput;
import org.knime.core.node.streamable.SharedContainerPortObject;
import org.knime.core.node.streamable.SharedContainerPortObjectSpec;
import org.knime.core.node.streamable.SharedPortObjectInput;
import org.knime.core.node.streamable.SharedPortObjectOutput;
import org.knime.core.node.workflow.ConnectionContainer;
import org.knime.core.node.workflow.FlowObjectStack;
import org.knime.core.node.workflow.SingleNodeContainer;

/**
 * A cash for {@link SharedContainerPortObject}s. Those port objects can be streamed even though they do not hold
 * a {@link BufferedDataTable}. The encapsulated object is shared between the producing and the consuming node and
 * is update by the producer and processed by the consumer multiple times. It is taken care of synchronization.
 *
 * @author Clemens von Schwerin, University of Ulm
 */
public class SharedContainerOutputCache extends AbstractOutputCache<SharedContainerPortObjectSpec> {

    private SharedPortObjectOutput<?> m_output;

    /**
     * Constructor.
     * @param nnc the underlying node container
     */
    public SharedContainerOutputCache(final SingleNodeContainer nnc) {
        super(nnc, SharedContainerPortObjectSpec.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FlowObjectStack getFlowObjectStack(final InputPortRole inputRole) throws InterruptedException {
        final ReentrantLock lock = getLock();
        lock.lockInterruptibly();
        try {
            getPortObject(); // wait for output to be populated
            return getSingleNodeContainer().createOutFlowObjectStack();
        } finally {
            lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PortOutput getPortOutput() {
        return getSharedPortOutput();
    }

    private <T extends Serializable>SharedPortObjectOutput<T> getSharedPortOutput() {
        if(m_output == null) {
            m_output = new SharedPortObjectOutput<T>();
        }
        return (SharedPortObjectOutput<T>) m_output;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PortObject getPortObjectMock() {
        try {
            return getPortObject();
        } catch (InterruptedException e) {
            throw new IllegalStateException("Interrupted before valid port object could be retreived.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PortInput getPortInput(final InputPortRole role, final ConnectionContainer cc) throws InterruptedException {
        //TODO check input role
        return getSharedPortInput();
    }

    private <T extends Serializable>SharedPortObjectInput<T> getSharedPortInput() {
         try {
            return new SharedPortObjectInput<T>((SharedContainerPortObject<T>) getPortObject());
        } catch (InterruptedException e) {
            throw new IllegalStateException("Interrupted before valid port object could be retreived.");
        }
    }

    /** Get the port object. It is always immediately available although the encapsulated object may be null.
     * @return the non-null object.
     * @throws InterruptedException When interrupted while waiting for it to be set.
     */
    PortObject getPortObject() throws InterruptedException {
        final Lock lock = getLock();
        lock.lockInterruptibly();
        try {
            return getSharedPortOutput().getPortObject();
        } finally {
            lock.unlock();
        }
    }

}
