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
 *   Apr 18, 2018 (Mark Ortmann, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.kafka.algorithms;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.StringValue;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.streamable.DataTableRowInput;
import org.knime.core.node.streamable.RowInput;
import org.knime.core.util.ThreadUtils.CallableWithContext;
import org.knime.kafka.settings.SettingsModelKafkaProducer;
import org.knime.kafka.settings.SettingsModelKafkaProducer.SendingType;

/**
 * Class to send messages to Kafka.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 *
 */
public final class KNIMEKafkaProducer {

    private static final String MISSING_COLUMN_EXCEPTION = "Message column not found";

    /** The connection properties. */
    private final Properties m_connectionProps;

    /** The properties for the Kafka producer. */
    private final Properties m_props;

    /** The list of topics to send messages to. */
    private final List<String> m_topic;

    /** The name of the column storing the messages that have to be send. */
    private final String m_msgCol;

    /** The connection validation timeout. */
    private final int m_conValTimeout;

    /** The kafkaProducer. */
    private KafkaProducer<String, String> m_producer;

    /** <code>True</code> if the producer is executed in transaction mode. */
    private final boolean m_transactionMode;

    /** The commit interval. -1 means that no intermediate transaction commits will be triggered. */
    private final int m_commitInterval;

    /** The {@link SendingType}. */
    private final SendingType m_sendingType;

    /** Reference to an exception that is thrown when sending asynchronously. */
    private final AtomicReference<Exception> m_exception;

    /** The asynchronous thread validating successful termination of all sends. */
    private final Thread m_asyncThread;

    /** The blocking queue storing the futures for each send record. */
    private final BlockingQueue<Object> m_asyncQueue;

    /** The close object used to stop the asynchronous validation callable. */
    private static final Object CLOSE = new Object();

    /**
     * Constructor.
     *
     * @param connectionProps the connection properties
     * @param conValTimeout the connection validation timeout
     * @param settings the @link {@link SettingsModelKafkaProducer} storing all settings
     */
    public KNIMEKafkaProducer(final Properties connectionProps, final int conValTimeout,
        final SettingsModelKafkaProducer settings) {
        this(connectionProps, settings.getProperties(), settings.getTopics(), settings.getMessageColumn(),
            conValTimeout, settings.useTransactions(), settings.getTransactionCommitInterval(),
            settings.getSendingType());
    }

    /**
     * Constructor.
     *
     * @param connectionProps the connection properties
     * @param producerProps the producer properties
     * @param topic a string representing a list of topics to the send the messages to
     * @param msgCol the name of the column storing that have to be send
     * @param conValTimeout the connection validation timeout
     * @param transactionMode defines whether whether messages have to be send transaction wise or not
     * @param commitInterval triggers a transaction commit after the specified number of read input rows (-1 results in
     *            a single transaction commit after the whole input has been processed)
     * @param sendingType the {@link SendingType}
     */
    private KNIMEKafkaProducer(final Properties connectionProps, final Properties producerProps, final String topic,
        final String msgCol, final int conValTimeout, final boolean transactionMode, final int commitInterval,
        final SendingType sendingType) {
        m_connectionProps = new Properties();
        m_connectionProps.putAll(connectionProps);
        m_props = new Properties();
        m_props.putAll(m_connectionProps);
        m_props.putAll(producerProps);
        m_topic = Arrays.asList(topic.trim().split("\\s*,\\s*", -1));
        m_msgCol = msgCol;
        m_conValTimeout = conValTimeout;
        m_transactionMode = transactionMode;
        m_commitInterval = commitInterval;
        m_exception = new AtomicReference<>();
        m_sendingType = sendingType;
        if (m_sendingType == SendingType.ASYNCHRONOUS) {
            m_asyncQueue = new LinkedBlockingQueue<>();
            m_asyncThread = new Thread(new FutureTask<Void>(new CallableWithContext<Void>() {

                @SuppressWarnings("unchecked")
                @Override
                protected Void callWithContext() throws Exception {
                    Object o;
                    while ((o = m_asyncQueue.take()) != CLOSE) {
                        try {
                            ((Future<RecordMetadata>)o).get();
                        } catch (final Exception e) {
                            m_exception.compareAndSet(null, e);
                            break;
                        }
                    }
                    return null;
                }
            }), "Async Kafka-Producer");
            m_asyncThread.start();
        } else {
            m_asyncQueue = null;
            m_asyncThread = null;
        }
    }

    /**
     * Start the execution, i.e. reading messages from the input and sending them to Kafka.
     *
     * @param exec the execution context
     * @param inData the input table holding the messages to send
     * @throws Exception- If the execution was canceled or the {@link KafkaProducer} had problems to sync or send new
     *             messages
     */
    public void execute(final ExecutionContext exec, final BufferedDataTable inData) throws Exception {
        execute(exec, new DataTableRowInput(inData));
    }

    /**
     * Start the execution, i.e. reading messages from the input and sending them to Kafka.
     *
     * @param exec the execution context
     * @param input the input to read the message content from
     * @throws Exception - If the execution was canceled or the {@link KafkaProducer} had problems to sync or send new
     *             messages
     */
    public void execute(final ExecutionContext exec, final RowInput input) throws Exception {
        // the input table spec
        final DataTableSpec inSpec = input.getDataTableSpec();

        // find the index of the column holding the messages to be send
        final int colIdx = inSpec.findColumnIndex(m_msgCol);
        if (colIdx < 0) {
            throw new InvalidSettingsException(MISSING_COLUMN_EXCEPTION);
        }

        // progress related counters
        long rowNo = 0;
        final long rowCnt;
        if (input instanceof DataTableRowInput) {
            rowCnt = ((DataTableRowInput)input).getRowCount();
        } else {
            rowCnt = -1;
        }

        // initialize the producer
        initProducer();

        if (m_transactionMode) {
            m_producer.initTransactions();
            m_producer.beginTransaction();
        }
        // the currently processed row
        DataRow row;

        int numSendRec = 0;

        while ((row = input.poll()) != null) {
            try {
                exec.checkCanceled();
                checkAsyncException();
                // update progress
                final String rowKey = row.getKey().toString();
                ++rowNo;
                final long rowNoFinal = rowNo;
                if (rowCnt <= 0) {
                    exec.setMessage(() -> "Writing row " + (rowNoFinal) + " (\"" + rowKey + "\")");
                } else {
                    exec.setProgress(rowNoFinal / (double)rowCnt,
                        () -> "Writing row " + (rowNoFinal) + " (\"" + rowKey + "\") of " + rowCnt);
                }

                // get the cell holding the message
                final DataCell cell = row.getCell(colIdx);
                if (cell.isMissing()) {
                    continue;
                }
                final String message = ((StringValue)cell).getStringValue();
                // send the message to all topics
                for (final String topic : m_topic) {
                    final ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
                    sendRecord(record);
                }
                if (m_transactionMode && m_commitInterval > 0 && ++numSendRec == m_commitInterval) {
                    numSendRec = 0;
                    m_producer.commitTransaction();
                    m_producer.beginTransaction();
                }
            } catch (final Exception e) {
                // abort the transaction if necessary
                if (m_transactionMode) {
                    m_producer.abortTransaction();
                }
                if (m_asyncThread != null) {
                    m_asyncThread.interrupt();
                }
                throw (e);
            }
        }
        // finish the transaction / flush the output
        if (m_transactionMode) {
            m_producer.commitTransaction();
        } else {
            m_producer.flush();
        }

    }

    /**
     * @throws Exception
     *
     */
    private void checkAsyncException() throws Exception {
        if (m_exception.get() != null) {
            throw m_exception.get();
        }
    }

    private void sendRecord(final ProducerRecord<String, String> record)
        throws InterruptedException, ExecutionException {
        switch (m_sendingType) {
            case ASYNCHRONOUS:
                m_asyncQueue.offer(m_producer.send(record));
                break;
            case SYCHRONOUS:
                m_producer.send(record).get();
                break;
            case FIRE_FORGET:
                m_producer.send(record);
                break;
            default:
                throw new IllegalArgumentException("There is send behaviour defined for this SendType");
        }
    }

    /**
     * Initializes the producer.
     */
    private void initProducer() throws InvalidSettingsException {
        if (m_producer == null) {
            KafkaConnectionValidator.testConnection(m_connectionProps, m_conValTimeout);
            m_producer = new KafkaProducer<>(m_props);
        }
    }

    /**
     * Closes the producer.
     *
     * @throws Exception
     */
    public void close() throws Exception {
        // if the producer is not null
        if (m_producer != null) {
            // TODO: If the nodes was executed in streaming mode
            // a canceled execution exception will mark the
            // thread interrupted, causing KafkaProducer to log an
            // error during close. Removing this flag stops the logging.
            // However a better solution would be to change the logging
            // level of the KafkaProducer in this case.
            if (Thread.currentThread().isInterrupted()) {
                if(m_asyncThread!=null) {
                    m_asyncThread.interrupt();
                }
                Thread.interrupted();
            }
            if (m_exception.get() == null && m_asyncThread != null) {
                m_asyncQueue.offer(CLOSE);
                m_asyncThread.join();
            }
            // close the producer
            try {
                m_producer.close();
            } catch (final Exception e) {
            } finally {
                m_producer = null;
            }
            checkAsyncException();
        }
    }
}
