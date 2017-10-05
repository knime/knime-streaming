/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME GmbH, Konstanz, Germany
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
 *   May 15, 2015 (wiswedel): created
 */
package org.knime.core.streaming.inoutput;

import org.knime.core.data.DataRow;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.streamable.DataTableRowInput;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.workflow.ConnectionContainer;

/**
 * Extension of {@link DataTableRowInput} that also sends UI events into the {@link ConnectionContainer}
 * (marching ants).
 *
 * @author Bernd Wiswedel, KNIME.com, Zurich, Switzerland
 */
public final class StagedTableRowInput extends DataTableRowInput {

    private ConnectionContainer m_cc;

    private long m_counter;

    private final BufferedDataTable m_table;

    /** Inits input.
     * @param table The table to read from.
     * @param cc The connection to send events to (only for display in the UI).
     */
    StagedTableRowInput(final BufferedDataTable table, final ConnectionContainer cc) {
        super(table);
        m_table = table;
        m_cc = CheckUtils.checkArgumentNotNull(cc, "Arg must not be null");
    }

    /** {@inheritDoc} */
    @Override
    public DataRow poll() throws InterruptedException {
        final DataRow next = super.poll();
        long count = next == null ? m_counter : ++m_counter;
        InMemoryRowCache.fireProgressEvent(m_cc, next != null, count);
        return next;
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        InMemoryRowCache.fireProgressEvent(m_cc, false, m_counter);
        super.close();
    }

    /** @return the table
     * @noreference This method is not intended to be referenced by clients. */
    public BufferedDataTable getTable() {
        return m_table;
    }

}
