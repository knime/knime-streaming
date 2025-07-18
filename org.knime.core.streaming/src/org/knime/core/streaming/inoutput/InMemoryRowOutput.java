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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.knime.core.data.DataRow;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.streamable.RowOutput;

/**
 * {@link RowOutput} for table ports. It passes the data into a {@link InMemoryRowCache}, which does all the
 * synchronization and sends it off to downstream nodes.
 *
 * @author Bernd Wiswedel, KNIME AG, Zurich, Switzerland
 */
public final class InMemoryRowOutput extends RowOutput {

    private final InMemoryRowCache m_rowCache;
    private List<DataRow> m_rows;

    InMemoryRowOutput(final InMemoryRowCache rowCache) {
        m_rowCache = rowCache;
        m_rows = new ArrayList<DataRow>(rowCache.getChunkSize());
    }

    /** {@inheritDoc} */
    @Override
    public void push(final DataRow row) throws InterruptedException {
        pushInternal(row, false);
    }

    private void pushInternal(final DataRow row, final boolean mayClose) throws InterruptedException {
        m_rows.add(row);
        if (m_rows.size() == m_rowCache.getChunkSize()) {
            if (m_rowCache.addChunk(m_rows, false, mayClose)) {
                assert mayClose : "Can't close output as flag is false";
                throw new OutputClosedException();
            }
            m_rows = new ArrayList<>(m_rowCache.getChunkSize());
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setFully(final BufferedDataTable table) throws InterruptedException {
        m_rowCache.setFully(table);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setInactive() {
        m_rowCache.setInactive();
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        try {
            m_rowCache.addChunk(m_rows, true, /* ignored when 2nd arg ist true */ false);
            m_rows = Collections.emptyList();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
