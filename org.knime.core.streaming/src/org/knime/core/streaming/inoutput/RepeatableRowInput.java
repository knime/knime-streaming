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
 *   Jul 4, 2016 (hornm): created
 */
package org.knime.core.streaming.inoutput;

import java.util.ArrayList;
import java.util.List;

import org.knime.core.data.DataRow;
import org.knime.core.data.DataTable;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowIterator;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.streamable.DataTableRowInput;
import org.knime.core.node.streamable.RowInput;

/**
 * A row input that wraps another {@link RowInput} or {@link DataTable} and allows one to repeat polling the rows after {@link #reset()}ing
 * it.
 * Rows that have been already polled from a wrapped row input are cached into {@link BufferedDataTable}s.
 *
 * @author Martin Horn
 */
public class RepeatableRowInput extends RowInput {

    private List<BufferedDataTable> m_tables = new ArrayList<BufferedDataTable>();

    private RowIterator m_currentIterator;

    private BufferedDataContainer m_currentTable;

    private ExecutionContext m_exec = null;

    private int m_tableIndex = 0;

    private RowInput m_wrappedRowInput;

    private DataTable m_wrappedTable = null;

    /**
     * Creates a new cached row input by wrapping the passed row input.
     *
     * @param rowInput the row input to wrap and possibly buffer
     * @param exec execution context required to create the intermediate buffered tables
     *
     */
    public RepeatableRowInput(final RowInput rowInput, final ExecutionContext exec) {
        m_wrappedRowInput = rowInput;
        m_exec = exec;
        reset();
    }

    /**
     * Creates a new repeatable row input by wrapping a data table.
     *
     * @param the data table to be wrapped
     */
    public RepeatableRowInput(final DataTable table) {
        m_wrappedTable = table;
        reset();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataTableSpec getDataTableSpec() {
        return m_wrappedRowInput.getDataTableSpec();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataRow poll() throws InterruptedException {
        if(m_wrappedTable != null) {
            //already 'cached' row input
            return m_wrappedRowInput.poll();
        }
        if (m_currentIterator != null) {
            //get row from cached table
            if (m_currentIterator.hasNext()) {
                return m_currentIterator.next();
            } else if (m_tableIndex + 1 < m_tables.size()) {
                m_tableIndex++;
                m_currentIterator = m_tables.get(m_tableIndex).iterator();
                return poll();
            } else {
                //no more cached rows, fall back to the wrapped row input
                m_currentIterator = null;
                return poll();
            }
        } else {
            //no cached row, poll from the wrapped row input
            DataRow row = m_wrappedRowInput.poll();
            if (row != null) {
                if (m_currentTable == null) {
                    m_currentTable = m_exec.createDataContainer(m_wrappedRowInput.getDataTableSpec());
                }
                m_currentTable.addRowToTable(row);
            } else {
                m_wrappedRowInput.close();
            }
            return row;
        }
    }

    /**
     * @return the wrapped row input
     */
    public RowInput getWrappedRowInput() {
        return m_wrappedRowInput;
    }

    /**
     * {@inheritDoc}
     *
     * Calling this method won't close the wrapped row input!
     */
    @Override
    public void close() {
        if (m_currentTable != null) {
            m_currentTable.close();
            m_tables.add(m_currentTable.getTable());
        }
        m_currentTable = null;
    }

    /**
     * Resets the row input. The next {@link #poll()}-call will than return again the very first row.
     */
    public void reset() {
        //re-create data table row input
        if (m_wrappedTable != null) {
            m_wrappedRowInput = new DataTableRowInput(m_wrappedTable);
            return;
        }
        close();
        m_tableIndex = 0;
        if (m_tables.size() > 0) {
            m_currentIterator = m_tables.get(0).iterator();
        } else {
            m_currentIterator = null;
        }
    }

}
