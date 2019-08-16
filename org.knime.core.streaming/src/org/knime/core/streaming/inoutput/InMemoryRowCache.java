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

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowIterator;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.inactive.InactiveBranchPortObject;
import org.knime.core.node.streamable.InputPortRole;
import org.knime.core.node.streamable.PortInput;
import org.knime.core.node.streamable.PortObjectInput;
import org.knime.core.node.streamable.PortOutput;
import org.knime.core.node.streamable.RowOutput;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.workflow.ConnectionContainer;
import org.knime.core.node.workflow.ConnectionProgress;
import org.knime.core.node.workflow.ConnectionProgressEvent;
import org.knime.core.node.workflow.FlowObjectStack;
import org.knime.core.streaming.SimpleStreamerNodeExecutionJob.NodeContainerCacheHandle;
import org.knime.core.streaming.SimpleStreamerNodeExecutionSettings;

/**
 * (Non-)Cache for table ports. Caching of the data into a {@link BufferedDataTable} is done if
 * <ul>
 * <li>any of the connected downstream nodes is not streamable or</li>
 * <li>the producing node forks into multiple branches that merge downstream again ('diamond').</li>
 * </ul>
 * In case the data can be streamed the cache holds a chunk of data and does not accept new
 * data until all consumers have fetched the current chunk.
 *
 * @author Bernd Wiswedel, KNIME AG, Zurich, Switzerland
 */
public final class InMemoryRowCache extends AbstractOutputCache<DataTableSpec> {

    private NodeContainerCacheHandle m_ncCacheHandle;

    private final BitSet m_hasConsumedCurrentChunkBits;

    private final BitSet m_closedConsumersBits;

    private final Condition m_acceptProduceCondition;

    private final Condition m_requireConsumeCondition;

    private final Condition m_requireFullDataConsumeCondition;

    private final Condition m_requirePrepareCondition;

    private final ExecutionContext m_context;

    private final boolean m_hasNonStreamableConsumer;

    private BufferedDataContainer m_stagingDataContainer;

    private BufferedDataTable m_stagedDataTable;

    private List<DataRow> m_currentChunk;

    private boolean m_isLast;

    private final int m_streamedConsumerCount;

    private int m_nrStreamConsumersCreated;

    private final int m_chunkSize;
    /**
     * A node is a diamond source if it has multiple output connections (from the same or different ports), which
     * downstream merge again. This can either happen directly at the connected successor or many layers downstream.
     * Such structures likely cause deadlock situations so the data at the producing node needs staging.
     */
    private boolean m_isDiamondSource;


    /**
     * New cache.
     * @param ncCacheHandle The node container along with some common pool of output caches so that they all
     * synchronize one when they can bail out as no more consumers consume
     * @param context to create BDT from in case a consumer needs full access.
     * @param settings the settings
     * @param nrStreamedConsumers Number of streaming consumers to be created - no data is accepted until all consumers
     *            have been created.
     * @param hasNonStreamableConsumer If the data is to be cached as a downstream node require full access.
     * @param isDiamondSource If node is branching (see member description for details).
     */
    public InMemoryRowCache(final NodeContainerCacheHandle ncCacheHandle, final ExecutionContext context,
        final SimpleStreamerNodeExecutionSettings settings, final int nrStreamedConsumers, final boolean hasNonStreamableConsumer, final boolean isDiamondSource) {
        super(ncCacheHandle.getSingleNodeContainer(), DataTableSpec.class);
        m_ncCacheHandle = ncCacheHandle;
        m_chunkSize = settings.getChunkSize();
        m_streamedConsumerCount = nrStreamedConsumers;
        m_context = CheckUtils.checkArgumentNotNull(context, "Exec Context must not be null");
        m_hasNonStreamableConsumer = hasNonStreamableConsumer;
        m_isDiamondSource = isDiamondSource;
        m_hasConsumedCurrentChunkBits = new BitSet(isDiamondSource ? 0 : m_streamedConsumerCount);
        m_closedConsumersBits = new BitSet(isDiamondSource ? 0 : m_streamedConsumerCount);
        final ReentrantLock lock = getLock();
        m_acceptProduceCondition = lock.newCondition();
        m_requireConsumeCondition = lock.newCondition();
        m_requireFullDataConsumeCondition = lock.newCondition();
        m_requirePrepareCondition = lock.newCondition();
    }

    /** {@inheritDoc} */
    @Override
    public void setPortObjectSpec(final DataTableSpec spec) {
        ReentrantLock lock = getLock();
        lock.lock();
        try {
            super.setPortObjectSpec(spec);
            if (shouldStageOuptut() && m_stagedDataTable == null) { // m_stagedDataTable != null if set by setFully
                CheckUtils.checkState(m_stagingDataContainer == null, "Must be null at this time");
                m_stagingDataContainer = m_context.createDataContainer(spec);
            }
        } finally {
            lock.unlock();
        }
    }

    private boolean shouldStageOuptut() {
        return m_hasNonStreamableConsumer || m_isDiamondSource;
    }

    /** {@inheritDoc} */
    @Override
    public PortInput getPortInput(final InputPortRole role, final ConnectionContainer cc, final ExecutionContext exec)
        throws InterruptedException {
        final ReentrantLock lock = getLock();
        lock.lockInterruptibly();
        try {
            if (role.isStreamable()) {
                if (m_isDiamondSource) { // don't stream to avoid deadlocks
                    PortObject stagedTable = waitForStagedTable();
                    if (stagedTable instanceof BufferedDataTable) {
                        return new StagedTableRowInput((BufferedDataTable)stagedTable, cc);
                    } else {
                        return new PortObjectInput(stagedTable);
                    }
                } else { // real streaming
                    int streamConsumersID = m_nrStreamConsumersCreated++;
                    getPortObjectSpec(); // wait for port object spec to be available.
                    m_acceptProduceCondition.signalAll();
                    return new InMemoryRowInput(streamConsumersID, cc, this);
                }
            } else {
                PortObject stagedTable = waitForStagedTable();
                if (stagedTable instanceof BufferedDataTable) {
                    InMemoryRowCache.fireProgressEvent(cc, false, ((BufferedDataTable)stagedTable).size());
                }
                return new PortObjectInput(stagedTable);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Waits until the table is assembled and then returns it.
     *
     * @return The table or an {@link InactiveBranchPortObject}, if input is inactive.
     * @throws InterruptedException If interrupted while waiting.
     */
    private PortObject waitForStagedTable() throws InterruptedException {
        assert getLock().isHeldByCurrentThread();
        while (m_stagedDataTable == null && !isInactive()) {
            m_requireFullDataConsumeCondition.await();
        }
        if (isInactive()) {
            return InactiveBranchPortObject.INSTANCE;
        } else {
            return m_stagedDataTable;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setInactive() {
        super.setInactive();
        final ReentrantLock lock = getLock();
        lock.lock();
        try {
            m_requireConsumeCondition.signalAll();
            m_requireFullDataConsumeCondition.signalAll();
            m_requirePrepareCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void prepare() throws InterruptedException {
        final ReentrantLock lock = getLock();
        lock.lockInterruptibly();
        try {
            while (m_currentChunk == null && m_stagedDataTable == null && !isInactive()) {
                m_requirePrepareCondition.await();
            }
        } finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public PortOutput getPortOutput() {
        final ReentrantLock lock = getLock();
        lock.lock();
        try {
            checkNotInUse();
            return new InMemoryRowOutput(this);
        } finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public PortObject getPortObjectMock() {
        final ReentrantLock lock = getLock();
        lock.lock();
        try {
            if (isInactive()) {
                return InactiveBranchPortObject.INSTANCE;
            }
            final DataTableSpec dts = getPortObjectSpecNoWait();
            CheckUtils.checkState(dts != null, "Spec not expected to be null at this point");
           return m_stagedDataTable != null ? m_stagedDataTable : m_context.createVoidTable(dts);
        } finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public FlowObjectStack getFlowObjectStack(final InputPortRole inputRole) throws InterruptedException {
        if (inputRole.isStreamable()) {
            return null;
        }
        final ReentrantLock lock = getLock();
        lock.lockInterruptibly();
        try {
            waitForStagedTable(); // wait for table to be available
            return getSingleNodeContainer().createOutFlowObjectStack();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Called by the producer to send a new chunk downstream. This method then either blocks (because the previous chunk
     * is still being processed), sets the argument chunk as current chunk or caches the chunk.
     *
     * @param rows The rows to add, never null but possibly empty. Row count should not exceed {@link #CHUNK_SIZE}.
     * @param isLast If this chunk is the last chunk and no more rows are expected
     * @param mayCloseOutput <code>true</code if this output and all others attached to the node may be closed in case
     *          there are no consumers waiting for further input
     * @return true if the producer can close the source because all streaming consumers are done (input closed)
     * @throws InterruptedException If interrupted while blocking.
     * @throws IllegalStateException If more rows are added while previous chunk was set to be the last one.
     */
    boolean addChunk(final List<DataRow> rows, final boolean isLast,
        final boolean mayCloseOutput) throws InterruptedException {
        CheckUtils.checkNotNull(rows, "Rows argument must not be null");
        // implication m_isLast --> isLast ---- can't reopen
        CheckUtils.checkState(!m_isLast || isLast, "Cannot re-open row cache - isLast flag was set previously");
        // subsequent calls with isLast==true are allowed if no rows are added
        CheckUtils.checkState(!m_isLast || rows.isEmpty(), "Previous chunk was last one - can't add new rows");
        final ReentrantLock lock = getLock();
        lock.lockInterruptibly();
        try {
            CheckUtils.checkState(getPortObjectSpec() != null,
                "Can't add rows to output as no spec was set -- computeFinalSpec probably returned null");
            if (m_isLast) {
                return false; // return value doesn't matter - but return value corresponds to the state of the consumer
            }
            while (m_currentChunk != null && !isCurrentChunkConsumed()) {
                m_acceptProduceCondition.await();
            }
            m_hasConsumedCurrentChunkBits.clear();
            m_hasConsumedCurrentChunkBits.or(m_closedConsumersBits);
            // all consumers are streamable and have called the #closeConsumer method
            // (e.g. a downstream row filter only accepting the first x rows)
            // signal to the source that no more output needs to be generated (via exception)
            // TODO this is currently harcoded to false
            boolean shouldCloseOutput = mayCloseOutput && !m_hasNonStreamableConsumer
                    && m_hasConsumedCurrentChunkBits.cardinality() >= m_streamedConsumerCount;
            if (shouldCloseOutput) {
                // can only close output if all other output caches (= ports) for that node are also done
                shouldCloseOutput = m_ncCacheHandle.closeOutput(this);
            }
            m_currentChunk = rows;
            m_isLast = isLast;
            if (m_stagingDataContainer != null) {
                assert !shouldCloseOutput : String.format("Data needs to be staged but all consumers are streamable "
                    + "and done (number threads waiting to lock: %d)", lock.getQueueLength());
                for (DataRow r : rows) {
                    m_stagingDataContainer.addRowToTable(r);
                    if (Thread.interrupted()) {
                        throw new InterruptedException();
                    }
                }
                if (isLast || shouldCloseOutput) {
                    m_stagingDataContainer.close();
                    m_stagedDataTable = m_stagingDataContainer.getTable();
                    // update spec (this also contains the domain)
                    setPortObjectSpec(m_stagedDataTable.getDataTableSpec());
                    m_stagingDataContainer = null;
                    m_requireFullDataConsumeCondition.signalAll();
                }
            }
            m_requireConsumeCondition.signalAll();
            m_requirePrepareCondition.signalAll();
            return shouldCloseOutput;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Alternative way to push data into the row cache. Used by nodes that use
     * {@link RowOutput#setFully(BufferedDataTable)}.
     *
     * @param table The table to push.
     * @throws InterruptedException When interrupted while processing/blocking.
     * @throws IllegalStateException if rows were added previously.
     */
    public void setFully(final BufferedDataTable table) throws InterruptedException {
        CheckUtils.checkArgumentNotNull(table, "Table must not be null");
        final ReentrantLock lock = getLock();
        lock.lockInterruptibly();
        try {
            checkNotInUse();
            if (shouldStageOuptut()) {
                if (m_stagingDataContainer != null) {
                    // computeFinalSpec returned a non-null spec so output was initialized
                    // the final table will overrule and we can discard the (empty) container
                    m_stagingDataContainer.close();
                    final BufferedDataTable tempEmptyTable = m_stagingDataContainer.getTable();
                    CheckUtils.checkState(tempEmptyTable.size() == 0L, "Can't set full table as rows have been "
                        + "previously added using 'push'");
                    m_context.clearTable(tempEmptyTable);
                    m_stagingDataContainer = null;
                }
                m_stagedDataTable = table;
                m_requireFullDataConsumeCondition.signalAll();
                m_requirePrepareCondition.signalAll();
            }
            setPortObjectSpec(table.getDataTableSpec());
            List<DataRow> rows = new ArrayList<DataRow>(m_chunkSize);
            RowIterator it = table.iterator();
            while (it.hasNext()) {
                if (rows.size() >= m_chunkSize) {
                    addChunk(rows, false, false);
                    rows = new ArrayList<DataRow>(m_chunkSize);
                }
                rows.add(it.next());
            }
            addChunk(rows, true, false);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get next chunk of data or null. Used by downstream streaming nodes.
     *
     * @param consumer The consumer -- used to set its flag so that when all are done we can accept the next chunk.
     * @return The next chunk or null when done.
     * @throws InterruptedException Interrupted while waiting for next chunk.
     */
    List<DataRow> getChunk(final InMemoryRowInput consumer) throws InterruptedException {
        final int consumerID = consumer.getConsumerID();
        final ReentrantLock lock = getLock();
        lock.lockInterruptibly();
        try {
            while (m_currentChunk == null || m_hasConsumedCurrentChunkBits.get(consumerID)) {
                if (m_isLast) {
                    return null;
                }
                m_requireConsumeCondition.await();
            }
            m_hasConsumedCurrentChunkBits.set(consumerID);
            final List<DataRow> currentChunk = m_currentChunk;
            if (isCurrentChunkConsumed()) {
                m_acceptProduceCondition.signalAll();
                if (m_isLast) {
                    m_currentChunk = null;
                }
            }
            return currentChunk;
        } finally {
            lock.unlock();
        }
    }

    /**
     * @param inMemoryRowInput
     */
    void closeConsumer(final InMemoryRowInput consumer) {
        final int consumerID = consumer.getConsumerID();
        final ReentrantLock lock = getLock();
        lock.lock();
        try {
            m_hasConsumedCurrentChunkBits.set(consumerID);
            m_closedConsumersBits.set(consumerID);
            if (isCurrentChunkConsumed()) {
                m_acceptProduceCondition.signalAll();
            }
        } finally {
            lock.unlock();
        }

    }

    /** @return the chunkSize as per settings. */
    int getChunkSize() {
        return m_chunkSize;
    }

    private boolean isCurrentChunkConsumed() {
        return m_isDiamondSource || m_hasConsumedCurrentChunkBits.cardinality() >= m_streamedConsumerCount;
    }

    private void checkNotInUse() {
        CheckUtils.checkState(m_currentChunk == null && !m_isLast, "Output cache already in use -- seen data before");
    }

    private static final NumberFormat FORMAT = NumberFormat.getIntegerInstance();

    static void fireProgressEvent(final ConnectionContainer cc, final boolean isInProgress, final long currentRow) {
        cc.progressChanged(new ConnectionProgressEvent(cc, new ConnectionProgress(isInProgress, () -> {
            synchronized (FORMAT) {
                return FORMAT.format(currentRow);
            }
        })));
    }

}
