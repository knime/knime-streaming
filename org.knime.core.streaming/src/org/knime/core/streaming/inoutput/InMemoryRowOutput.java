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

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.DataRowBuffer;
import org.knime.core.data.v2.RowRead;
import org.knime.core.data.v2.RowWrite;
import org.knime.core.data.v2.RowWriteCursor;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.streamable.RowOutput;

/**
 * {@link RowOutput} for table ports. It passes the data into a {@link InMemoryRowCache}, which does all the
 * synchronization and sends it off to downstream nodes.
 *
 * @author Bernd Wiswedel, KNIME AG, Zurich, Switzerland
 */
public final class InMemoryRowOutput extends RowOutput {

    private final DataTableSpec m_spec;

    private final InMemoryRowCache m_rowCache;

    private DataRowBuffer m_writeBatch;

    private RowWriteCursor m_writeCursor;

    private final int m_numColumns;

    private DataRow m_row;

    private final RowRead m_read;


    InMemoryRowOutput(final InMemoryRowCache rowCache) {
        m_rowCache = rowCache;
        m_spec = rowCache.getPortObjectSpecNoWait();
        m_writeBatch = InMemoryRowCache.newStreamingWriteBatch(m_spec);
        m_writeCursor = m_writeBatch.createCursor();
        m_numColumns = rowCache.getPortObjectSpecNoWait().getNumColumns();
        m_read = RowRead.suppliedBy(() -> m_row, m_numColumns);
    }

    @Override
    public InterruptibleRowWriteCursor asWriteCursor(final DataTableSpec spec) {

        return new InterruptibleRowWriteCursor() {

            @Override
            public RowWrite forward() {
                if (m_writeBatch.size() == m_rowCache.getChunkSize()) {
                    try {
                        closeBatch(spec, false);
                    } catch (final InterruptedException e) {
                        throw ExceptionUtils.asRuntimeException(e);
                    }
                }
                return m_writeCursor.forward();
            }

            @Override
            public void close() {
                try {
                    closeBatch(spec, false);
                } catch (final InterruptedException e) {
                    throw ExceptionUtils.asRuntimeException(e);
                }
            }

            @Override
            public boolean canForward() {
                return m_writeCursor.canForward();
            }

        };
    }

    @Override
    public void push(final DataRow row) throws InterruptedException {
        if (m_writeBatch.size() == m_rowCache.getChunkSize()) {
            closeBatch(m_spec, false);
        }
        final var write = m_writeCursor.forward();
        m_row = row;
        write.setFrom(m_read);
    }

    private void closeBatch(final DataTableSpec spec, final boolean mayClose) throws InterruptedException {
        if (m_spec != spec) {
            throw new IllegalStateException("Passed different spec to close batch than what output was created with");
        }
        m_writeCursor.close();
        if (m_rowCache.addChunk(m_writeBatch.finish(), false, mayClose)) {
            assert mayClose : "Can't close output as flag is false";
            throw new OutputClosedException();
        }
        m_writeBatch = InMemoryRowCache.newStreamingWriteBatch(spec);
    }

    @Override
    public void setFully(final BufferedDataTable table) throws InterruptedException, CanceledExecutionException {
        m_rowCache.setFully(table);
    }

    @Override
    public void setInactive() {
        m_rowCache.setInactive();
    }

    @Override
    public void close() {
        try {
            // potentially called multiple times, so should be idempotent
            if (m_writeBatch != null) {
                m_rowCache.addChunk(m_writeBatch.finish(), true, //
                    /* mayBeClosed: ignored when 2nd arg ist true, since last batch implies it may be closed */ false);
                m_writeBatch = null;
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
