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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataTableSpecCreator;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.json.JSONCell;
import org.knime.core.data.json.JSONCellFactory;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.streamable.BufferedDataTableRowOutput;
import org.knime.core.node.streamable.RowOutput;

/**
 * Class to consume messages from Kafka.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 *
 */

public final class KNIMEKafkaConsumer {

    /** The number of retries before stopping to reconnect to Kafka. */
    private static final int RETRIES = 3;

    /** The connection properties. */
    private final Properties m_connectionProps;

    /** The properties for the Kafka consumer. */
    private final Properties m_properties;

    /** The topics (list) or pattern to subscribe to. */
    private final String m_topics;

    /** <code>True</code> if the topics string represents a pattern. */
    private final boolean m_isPattern;

    /** The maximum number of empty polls before stopping the execution. */
    private final long m_maxEmptyPolls;

    /** The connection validation timeout. */
    private final int m_conValTimeout;

    /** <code>True</code> if the message history has to be ignored . */
    private boolean m_ignoreHistory;

    /** <code>True</code> if the message has to be converted to a JSON cell. */
    private final boolean m_convertToJSON;

    /**
     * <code>True</code> if the a column has to be append to the table informing about the topic the message originated
     * from.
     */
    private final boolean m_appendTopic;

    /** <code>True</code> if the consumer has not polled any message for max number of empty polls many times. */
    private boolean m_starved;

    /** The message offset. */
    private long m_offset;

    /** The batch size. */
    private long m_batchSize;

    /** The poll timeout. */
    private long m_pollTimeout;

    /** The KafkaConsumer . */
    private KafkaConsumer<Long, String> m_consumer;

    /** <code>True</code> if the KafkaConsumer was interrupted. */
    private boolean m_wasInterrupted;

    /**
     * Constructor.
     *
     * @param builder the builder storing the configuration
     */
    private KNIMEKafkaConsumer(final Builder builder) {
        m_connectionProps = builder.m_connectionProps;
        m_properties = new Properties();
        m_properties.putAll(m_connectionProps);
        m_properties.putAll(builder.m_consumerProps);
        m_conValTimeout = builder.m_conValTimeout;
        m_topics = builder.m_topics;
        m_isPattern = builder.m_isPattern;
        m_maxEmptyPolls = builder.m_maxEmptyPolls;
        m_ignoreHistory = builder.m_ignoreHistory;
        m_convertToJSON = builder.m_convertToJSON;
        m_appendTopic = builder.m_appendTopic;
        m_offset = builder.m_offset;
        m_batchSize = builder.m_batchSize;
        m_pollTimeout = builder.m_pollTimeout;
        m_starved = false;
        m_consumer = null;
        m_wasInterrupted = false;
    }

    /**
     * Builder used to create an instance of {@link KNIMEKafkaConsumer}.
     *
     * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
     */
    public static class Builder

    {
        // Required parameters
        /** The connection properties. */
        private final Properties m_connectionProps;

        /** The consumer properties. */
        private final Properties m_consumerProps;

        /** The topics (list) or pattern to subscribe to. */
        private final String m_topics;

        /** <code>True</code> if the topics string represents a pattern. */
        private final boolean m_isPattern;

        /** The maximum number of empty polls before stopping the execution. */
        private final long m_maxEmptyPolls;

        /** The connection validation timeout. */
        private final int m_conValTimeout;

        // Optional parameters
        /** <code>True</code> if the message history has to be ignored . */
        private boolean m_ignoreHistory = false;

        /** <code>True</code> if the message has to be converted to a JSON cell. */
        private boolean m_convertToJSON = false;

        /**
         * <code>True</code> if the a column has to be append to the table informing about the topic the message
         * originated from.
         */
        private boolean m_appendTopic = false;

        /** The message offset. */
        private long m_offset = 0;

        /** The batch size. */
        private long m_batchSize = 50;

        /** The poll timeout. */
        private long m_pollTimeout = 1000;

        /**
         * Constructor.
         *
         * @param connectionProps the connection properties
         * @param consumerProps the consumer properties
         * @param topics the topics (a list or pattern) the {@link KafkaConsumer} subscribes to
         * @param isPattern <code>True</code> if topics is a pattern
         * @param maxEmptyPools the maximum allowed number of successive empty polls before the {@link KafkaConsumer}
         *            stops its execution
         * @param conValTimeout the connection validation timeout
         */
        public Builder(final Properties connectionProps, final Properties consumerProps, final String topics,
            final boolean isPattern, final long maxEmptyPools, final int conValTimeout) {
            m_connectionProps = new Properties();
            m_connectionProps.putAll(connectionProps);
            m_consumerProps = new Properties();
            m_consumerProps.putAll(consumerProps);
            m_topics = topics;
            m_isPattern = isPattern;
            m_maxEmptyPolls = maxEmptyPools;
            m_conValTimeout = conValTimeout;
        }

        /**
         * Sets the ignore history flag. If <code>True</code> the consumer skips all unread messages.
         *
         * @param ignoreHistory the ignore history flag
         * @return the builder itself
         */
        public Builder ignoreHistory(final boolean ignoreHistory) {
            m_ignoreHistory = ignoreHistory;
            return this;
        }

        /**
         * Sets the convert to JSON flag. If <code>True</code> the message is converted to a {@link JSONCell} cell and a
         * {@link StringCell} otherwise.
         *
         * @param convertToJSON the convert to JSON flag
         * @return the builder itself
         */
        public Builder convertToJSON(final boolean convertToJSON) {
            m_convertToJSON = convertToJSON;
            return this;
        }

        /**
         * Sets the append topic flag. If <code>True</code> an additional column is added to the output informing about
         * the topic the message originated from.
         *
         * @param append the append topic flag
         * @return the builder itself
         */
        public Builder appendTopic(final boolean append) {
            m_appendTopic = append;
            return this;
        }

        /**
         * Set the offset row id offset.
         *
         * @param offset the number of the first row in the output
         * @return the builder itself
         */
        public Builder setOffset(final long offset) {
            m_offset = offset;
            return this;
        }

        /**
         * Sets the batch size, i.e. the maximum number of rows in the output table.
         *
         * @param batchSize the batch size
         * @return the builder itself
         */
        public Builder setBatchSize(final long batchSize) {
            m_batchSize = batchSize;
            return this;
        }

        /**
         * Sets the poll timeout used by the {@link KafkaConsumer}, see {@link KafkaConsumer#poll(long)}.
         *
         * @param pollTimeout
         * @return the builder itself
         */
        public Builder setPollTimeout(final long pollTimeout) {
            m_pollTimeout = pollTimeout;
            return this;
        }

        /**
         * Builds the {@link KNIMEKafkaConsumer}.
         *
         * @return the properly initialized {@link KNIMEKafkaConsumer}
         */
        public KNIMEKafkaConsumer build() {
            return new KNIMEKafkaConsumer(this);
        }

    }

    /**
     * Returns <code>True</code> if the execution of the consumer was stopped due to too many successive empty polls.
     *
     * @return <code>True</code> if no new messages were found
     */
    public boolean isStarved() {
        return m_starved;
    }

    /**
     * Closes the consumer.
     */
    public void close() {
        reset();
    }

    /**
     * Start the execution, i.e. polling messages from Kafka and writing them to the output table.
     *
     * @param exec the execution context
     * @return the output table storing the polled messages
     * @throws Exception - If the execution was canceled or the {@link KafkaConsumer} had problems synching or polling
     *             new messages
     */
    public BufferedDataTable execute(final ExecutionContext exec) throws Exception {
        final BufferedDataTableRowOutput output = new BufferedDataTableRowOutput(
            exec.createDataContainer(createOutTableSpec(m_convertToJSON, m_appendTopic)));
        execute(exec, output);
        return output.getDataTable();
    }

    /**
     * Start the execution, i.e. polling messages from Kafka and writing them to the row output.
     *
     * @param exec the execution context
     * @param output the output to write the messages to
     * @throws Exception - If the execution was canceled or the {@link KafkaConsumer} had problems to sync or poll new
     *             messages
     */
    public void execute(final ExecutionContext exec, final RowOutput output) throws Exception {
        boolean done = false;
        long noRecordsCount = 0;
        int retries = 0;
        int numOfPolledRec = 0;
        // flag used to omit ignoring the history in case of a reconnect
        boolean ignoreHistory = m_ignoreHistory;
        // map used for syncing
        final Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();

        // while there are more messages and we were able to reconnect to Kafka
        while (!done) {
            try {
                // check for user interrupted
                exec.checkCanceled();

                // reset offsetmap
                offsetMap.clear();

                // init the consumer
                initConsumer(ignoreHistory);

                // poll the messages
                final ConsumerRecords<Long, String> consumerRecords = m_consumer.poll(m_pollTimeout);

                // check if we have polled messages no records for more than the allowed number of times
                if (consumerRecords.count() == 0) {
                    // break the loop if necessary
                    done = m_maxEmptyPolls >= 0 && ++noRecordsCount > m_maxEmptyPolls;
                    // set the starved flag to true (indicates that there were no more messages)
                    m_starved = done;
                    continue;
                }

                // if we found new messages reset the counter
                noRecordsCount = 0;

                // push all messages to the output table
                for (final ConsumerRecord<Long, String> record : consumerRecords) {
                    output.push(convert(record));
                    // add the offset to the map
                    offsetMap.put(new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1));
                    // if the number of messages we sent
                    if (++numOfPolledRec == m_batchSize) {
                        done = true;
                        break;
                    }
                }

                // commit the offsets w.r.t. to the processed messages
                m_consumer.commitSync(offsetMap);

                // update flags and counters
                ignoreHistory = false;
                retries = 0;
            } catch (final CommitFailedException e) {
                // reset the consumer
                reset();

                // if we reached the max number of retries give up
                if (++retries == RETRIES) {
                    output.close();
                    throw e;
                } else {
                    // otherwise give it another try
                    exec.setMessage("Kafka consumer encountered a problem. Trying to reconnect (" + retries + " out of "
                        + RETRIES + " attempts)");
                    continue;
                }
            } catch (final InterruptException e) {
                // set interrupted flag and wake up the consumer
                m_wasInterrupted = true;
                m_consumer.wakeup();
            } catch (final CanceledExecutionException e) {
                // this is preceded by an interrupt therefore set this flag
                m_wasInterrupted = true;
                throw e;
            }
        }
        // close the output
        output.close();
    }

    /**
     * Resets the KafkaConsumer.
     */
    private void reset() {
        if (m_consumer != null) {
            if (m_wasInterrupted) {
                Thread.interrupted();
            }
            try {
                m_consumer.close();
            } catch (final Exception e) {
            } finally {
                m_consumer = null;
            }
        }
    }

    /**
     * Creates the output table spec.
     *
     * @param useJSON <code>True</code> if the messages are converted to {@link JSONCell}
     * @param appendTopic <code>True</code> if a column informing about the origin of the messages has to be added
     * @return the output table spec
     */
    public static DataTableSpec createOutTableSpec(final boolean useJSON, final boolean appendTopic) {
        final DataTableSpecCreator specCreator = new DataTableSpecCreator();
        final DataType type;
        if (useJSON) {
            type = JSONCell.TYPE;
        } else {
            type = StringCell.TYPE;
        }
        final DataColumnSpecCreator cellSpecCreator = new DataColumnSpecCreator("Message", type);
        specCreator.addColumns(cellSpecCreator.createSpec());
        if (appendTopic) {
            cellSpecCreator.setName("Topic");
            cellSpecCreator.setType(StringCell.TYPE);
            specCreator.addColumns(cellSpecCreator.createSpec());
        }
        return specCreator.createSpec();
    }

    /**
     * Initializes the KnimeConsumer.
     *
     * @param ignoreHistory <code>True</code> if unprocessed messages have to be ignored
     * @return the {@link KafkaConsumer}
     */
    private KafkaConsumer<Long, String> initConsumer(final boolean ignoreHistory) throws InvalidSettingsException {
        if (m_consumer != null) {
            return m_consumer;
        }
        // validate the connection before creating a consumer
        KafkaConnectionValidator.validateConnection(m_connectionProps, m_conValTimeout);

        // initialize the consumer
        m_consumer = new KafkaConsumer<>(m_properties);
        final ConsumerRebalanceListener crl = new ConsumerListener(ignoreHistory);
        if (m_isPattern) {
            m_consumer.subscribe(Pattern.compile(m_topics), crl);
        } else {
            m_consumer.subscribe(Arrays.asList(m_topics.trim().split("\\s*,\\s*", -1)), crl);
        }
        return m_consumer;
    }

    /**
     * Converts a message to a {@link StringCell} or {@link JSONCell}.
     *
     * @param record the record to convert
     * @return the message represented by a {@link StringCell} or {@link JSONCell}
     * @throws IOException - if an error occurred while converting to JSON
     */
    private DataRow convert(final ConsumerRecord<Long, String> record) throws IOException {
        DataCell msgCell;
        if (m_convertToJSON) {
            msgCell = JSONCellFactory.create(record.value(), true);
        } else {
            msgCell = new StringCell(record.value());
        }
        if (m_appendTopic) {
            return new DefaultRow(RowKey.createRowKey(m_offset++), msgCell, new StringCell(record.topic()));
        }
        return new DefaultRow(RowKey.createRowKey(m_offset++), msgCell);
    }

    /**
     * Listener the skips all unread messages if necessary.
     *
     * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
     */
    private class ConsumerListener implements ConsumerRebalanceListener {

        private final boolean m_seekToEnd;

        private ConsumerListener(final boolean seekToEnd) {
            m_seekToEnd = seekToEnd;
        }

        @Override
        public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
        }

        @Override
        public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
            if (m_seekToEnd) {
                m_consumer.seekToEnd(partitions);
            }
        }
    }
}
