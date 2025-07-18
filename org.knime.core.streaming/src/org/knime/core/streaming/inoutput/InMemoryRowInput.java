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
package org.knime.core.streaming.inoutput;

import java.util.List;

import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.streamable.RowInput;
import org.knime.core.node.workflow.ConnectionContainer;

/**
 * {@link RowInput} passed to streaming nodes. It reads from {@link InMemoryRowCache}.
 *
 * @author Bernd Wiswedel, KNIME AG, Zurich, Switzerland
 */
public final class InMemoryRowInput extends RowInput {

    private final int m_consumerID;
    private final InMemoryRowCache m_rowCache;
    private final ConnectionContainer m_connection;
    private List<DataRow> m_currentChunk;
    private int m_iteratorIndex;
    private long m_currentRowCount;

    InMemoryRowInput(final int consumerID, final ConnectionContainer cc, final InMemoryRowCache rowCache) {
        m_consumerID = consumerID;
        m_connection = cc;
        m_rowCache = rowCache;
        InMemoryRowCache.fireProgressEvent(m_connection, false, 0);
    }

    /** @return the consumerID */
    int getConsumerID() {
        return m_consumerID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isInactive() throws InterruptedException {
        m_rowCache.prepare();
        return m_rowCache.isInactive();
    }

    /** {@inheritDoc} */
    @Override
    public DataTableSpec getDataTableSpec() {
        try {
            return m_rowCache.getPortObjectSpec();
        } catch (InterruptedException e) {
            assert false : "Spec was supposed to be available already at construction time";
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public DataRow poll() throws InterruptedException {
        if (m_currentChunk == null || m_iteratorIndex >= m_currentChunk.size()) {
            m_currentChunk = m_rowCache.getChunk(this);
            m_iteratorIndex = 0;
        }
        if (m_currentChunk == null || m_iteratorIndex >= m_currentChunk.size()) {
            InMemoryRowCache.fireProgressEvent(m_connection, false, m_currentRowCount);
            return null;
        } else {
            InMemoryRowCache.fireProgressEvent(m_connection, true, m_currentRowCount++);
            return m_currentChunk.get(m_iteratorIndex++);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        m_rowCache.closeConsumer(this);
        InMemoryRowCache.fireProgressEvent(m_connection, false, m_currentRowCount);
    }

}
