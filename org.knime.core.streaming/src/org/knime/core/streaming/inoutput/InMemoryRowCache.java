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
package org.knime.core.streaming.inoutput;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.util.CheckUtils;

/**
 * A cache that gets filled by a producer node (upstream) and that is consumed by multiple (0-n) consumer nodes
 * (down-stream). If all consumers have fetched the current chunk that chunk will be released and new data from the
 * producer is accepted.
 *
 * @author Bernd Wiswedel, KNIME.com, Zurich, Switzerland
 */
public final class InMemoryRowCache {

    private final int m_nrConsumers;
    private List<DataRow> m_currentChunk;
    private final Set<InMemoryRowInput> m_consumers;
    private final ReentrantLock m_lock;
    private final Condition m_acceptProduceCondition;
    private final Condition m_requireConsumeCondition;
    private final DataTableSpec m_spec;
    private boolean m_isLast;

    /** New cache based on a number of consumers.
     * @param nrConsumers ... */
    public InMemoryRowCache(final int nrConsumers, final DataTableSpec spec) {
        m_spec = spec;
        CheckUtils.checkArgument(nrConsumers >= 0, "Consumer count not >= 0: " + nrConsumers);
        m_consumers = new HashSet<>();
        m_nrConsumers = nrConsumers;
        m_lock = new ReentrantLock();
        m_acceptProduceCondition = m_lock.newCondition();
        m_requireConsumeCondition = m_lock.newCondition();
    }

    public InMemoryRowInput createRowInput() {
        return new InMemoryRowInput(m_spec, this);
    }

    public InMemoryRowOutput createRowOutput() {
        return new InMemoryRowOutput(this);
    }

    public void addChunk(final List<DataRow> rows, final boolean isLast) throws InterruptedException {
        CheckUtils.checkState(!m_isLast, "Previous chunk was already the last one");
        CheckUtils.checkNotNull(rows, "Rows argument must not be null");
        m_lock.lockInterruptibly();
        try {
            while (m_currentChunk != null && m_consumers.size() < m_nrConsumers) {
                m_acceptProduceCondition.await();
            }
            m_consumers.clear();
            m_currentChunk = rows;
            m_isLast = isLast;
            m_requireConsumeCondition.signalAll();
        } finally {
            m_lock.unlock();
        }
    }

    public List<DataRow> getChunk(final InMemoryRowInput consumer) throws InterruptedException {
        m_lock.lockInterruptibly();
        try {
            while(m_currentChunk == null) {
                if (m_isLast) {
                    return null;
                } else {
                    m_requireConsumeCondition.await();
                }
            }
            while (!m_consumers.add(consumer)) {
                m_requireConsumeCondition.await();
            }
            final List<DataRow> currentChunk = m_currentChunk;
            if (m_consumers.size() == m_nrConsumers) {
                m_acceptProduceCondition.signalAll();
                if (m_isLast) {
                    m_currentChunk = null;
                }
            }
            return currentChunk;
        } finally {
            m_lock.unlock();
        }
    }

}
