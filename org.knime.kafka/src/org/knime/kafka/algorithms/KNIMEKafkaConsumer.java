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
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataTableSpecCreator;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.IntCell.IntCellFactory;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.LongCell.LongCellFactory;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.def.StringCell.StringCellFactory;
import org.knime.core.data.json.JSONCell;
import org.knime.core.data.json.JSONCellFactory;
import org.knime.core.data.time.zoneddatetime.ZonedDateTimeCellFactory;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.streamable.BufferedDataTableRowOutput;
import org.knime.core.node.streamable.RowOutput;
import org.knime.time.util.DateTimeType;

/**
 * Class to consume messages from Kafka.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 *
 */

public final class KNIMEKafkaConsumer {

    /**
     * Enum specifying the execution break condition.
     *
     * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
     */
    public enum BreakCondition {

            /** Indicates that the consumer shall stop polling messages after a certain number of messages. */
            MSG_COUNT("<html>Stop when number of<br/>consumed messages <br/>exceeds:</html>"),

            /** Indicates that the consumer should only poll messages before a certain time. */
            TIME("<html>Stop when message<br/>timestamp exceeds:</html>");

        /** Missing name exception. */
        private static final String NAME_MUST_NOT_BE_NULL = "Name must not be null";

        /** IllegalArgumentException prefix. */
        private static final String ARGUMENT_EXCEPTION_PREFIX = "No BreakCondition constant with name: ";

        private final String m_name;

        /**
         * Constructor
         *
         * @param name the enum name
         */
        private BreakCondition(final String name) {
            m_name = name;
        }

        @Override
        public String toString() {
            return m_name;
        }

        /**
         * Returns the enum for a given String
         *
         * @param name the enum name
         * @return the enum
         * @throws InvalidSettingsException if the given name is not associated with an {@link BreakCondition} value
         */
        public static BreakCondition getEnum(final String name) throws InvalidSettingsException {
            if (name == null) {
                throw new InvalidSettingsException(NAME_MUST_NOT_BE_NULL);
            }
            return Arrays.stream(values()).//
                filter(t -> t.m_name.equals(name))//
                .findFirst()//
                .orElseThrow(() -> new InvalidSettingsException(ARGUMENT_EXCEPTION_PREFIX + name));
        }
    }

    private static final DataColumnSpec[] RECORD_COL_SPECS = new DataColumnSpec[]{//
        createColSpec("Topic", StringCell.TYPE)//
        , createColSpec("Partition", IntCell.TYPE)//
        , createColSpec("Offset", LongCell.TYPE)//
        , createColSpec("Message creation date", DateTimeType.ZONED_DATE_TIME.getDataType())//
    };

    /** The connection properties. */
    private final Properties m_connectionProps;

    /** The properties for the Kafka consumer. */
    private final Properties m_properties;

    /** The topics (list) or pattern to subscribe to. */
    private final String m_topics;

    /** <code>True</code> if the topics string represents a pattern. */
    private final boolean m_isPattern;

    /** The connection validation timeout. */
    private final int m_conValTimeout;

    /** The time up until messages have to be consumed. */
    private final Long m_time;

    /** <code>True</code> if the message has to be converted to a JSON cell. */
    private final boolean m_convertToJSON;

    /** <code>True</code> if columns have to be append to the table storing information about the record. */
    private final boolean m_appendRecordInfo;

    /** The row id offset. */
    private long m_rowIdOffset;

    /** The total number of messages to be consumed. */
    private long m_totNumMsgs;

    /** The poll timeout. */
    private long m_pollTimeout;

    /** The consumption break condition. */
    private final BreakCondition m_breakCond;

    /** The zone id. */
    private final ZoneId m_zoneId;

    /** The KafkaConsumer . */
    private KafkaConsumer<String, String> m_consumer;

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
        m_time = builder.m_time;
        m_topics = builder.m_topics;
        m_isPattern = builder.m_isPattern;
        m_convertToJSON = builder.m_convertToJSON;
        m_appendRecordInfo = builder.m_appendRecordInfo;
        m_rowIdOffset = builder.m_rowIdOffset;
        m_totNumMsgs = builder.m_totNumMsgs;
        m_pollTimeout = builder.m_pollTimeout;
        m_breakCond = builder.m_breakCond;
        m_zoneId = builder.m_zoneId;
        m_consumer = null;
    }

    /**
     * Constructs a builder to create a {@link KNIMEKafkaConsumer} that polls the given number of messages.
     *
     * @param connectionProps the connection properties
     * @param consumerProps the consumer properties
     * @param topics the topics (a list or pattern) the {@link KafkaConsumer} subscribes to
     * @param isPattern <code>True</code> if topics is a pattern
     * @param conValTimeout the connection validation timeout
     * @param totNumMsgs the total number of messages to be polled
     * @return the builder with the given break condition.
     */
    public static Builder getMsgCountBuilder(final Properties connectionProps, final Properties consumerProps,
        final String topics, final boolean isPattern, final int conValTimeout, final long totNumMsgs) {
        return new Builder(connectionProps, consumerProps, topics, isPattern, conValTimeout, BreakCondition.MSG_COUNT,
            totNumMsgs, -1);
    }

    /**
     * Constructs a builder to create a {@link KNIMEKafkaConsumer} that polls up until the given point in time.
     *
     * @param connectionProps the connection properties
     * @param consumerProps the consumer properties
     * @param topics the topics (a list or pattern) the {@link KafkaConsumer} subscribes to
     * @param isPattern <code>True</code> if topics is a pattern
     * @param conValTimeout the connection validation timeout
     * @param time the time up until messages have to be consumed
     * @return the builder with the given break condition.
     *
     */
    public static Builder getDateBuilder(final Properties connectionProps, final Properties consumerProps,
        final String topics, final boolean isPattern, final int conValTimeout, final long time) {
        return new Builder(connectionProps, consumerProps, topics, isPattern, conValTimeout, BreakCondition.TIME, -1,
            time);
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
        final Properties m_connectionProps;

        /** The consumer properties. */
        final Properties m_consumerProps;

        /** The topics (list) or pattern to subscribe to. */
        final String m_topics;

        /** <code>True</code> if the topics string represents a pattern. */
        final boolean m_isPattern;

        /** The connection validation timeout. */
        final int m_conValTimeout;

        /** The consumption break condition. */
        final BreakCondition m_breakCond;

        /** The total number of messages to be consumed. */
        final long m_totNumMsgs;

        /** The time up until messages have to be consumed. */
        final long m_time;

        // Optional parameters
        /** <code>True</code> if the message has to be converted to a JSON cell. */
        boolean m_convertToJSON = false;

        /** <code>True</code> if columns have to be append to the table storing information about the record. */
        boolean m_appendRecordInfo = false;

        /** The row id offset. */
        long m_rowIdOffset = 0;

        /** The poll timeout. */
        long m_pollTimeout = 1000;

        /** The zone id. */
        private ZoneId m_zoneId = ZoneId.systemDefault();

        /**
         * Constructor.
         *
         * @param connectionProps the connection properties
         * @param consumerProps the consumer properties
         * @param topics the topics (a list or pattern) the {@link KafkaConsumer} subscribes to
         * @param isPattern <code>True</code> if topics is a pattern
         * @param conValTimeout the connection validation timeout
         * @param breakCondition the consumption break condition
         * @param totNumMsgs the total number of messages to be polled
         * @param time the time up until messages have to be consumed
         */
        Builder(final Properties connectionProps, final Properties consumerProps, final String topics,
            final boolean isPattern, final int conValTimeout, final BreakCondition breakCondition,
            final long totNumMsgs, final long time) {
            m_connectionProps = new Properties();
            m_connectionProps.putAll(connectionProps);
            m_consumerProps = new Properties();
            m_consumerProps.putAll(consumerProps);
            m_topics = topics;
            m_isPattern = isPattern;
            m_conValTimeout = conValTimeout;
            m_breakCond = breakCondition;
            m_totNumMsgs = totNumMsgs;
            m_time = time;
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
         * Sets the append records information flag. If <code>True</code> additional columns are added to the output
         * storing message information.
         *
         * @param append the append topic flag
         * @return the builder itself
         */
        public Builder appendRecordInfo(final boolean append) {
            m_appendRecordInfo = append;
            return this;
        }

        /**
         * Set the offset row id offset.
         *
         * @param rowIdOffset the number of the first row in the output
         * @return the builder itself
         */
        public Builder setRowIdOffset(final long rowIdOffset) {
            m_rowIdOffset = rowIdOffset;
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
         * Sets the zone id used to create the message timestamp.
         *
         * @param zoneId the zone id
         * @return the builder itself
         */
        public Builder setZoneId(final ZoneId zoneId) {
            m_zoneId = zoneId;
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
     * Closes the consumer.
     */
    public void close() {
        if (m_consumer != null) {
            // TODO: If the nodes was executed in streaming mode
            // a canceled execution exception will mark the
            // thread interrupted, causing KafkaConsumer to log an
            // error during close. Removing this flag stops the logging.
            // However a better solution would be to change the logging
            // level of the KafkaConsumer in this case.
            if (Thread.currentThread().isInterrupted()) {
                Thread.interrupted();
            }
            try {
                m_consumer.close();
            } catch (final Exception e) {
                // Exception is logged anyway by the KafkaConsumer
            } finally {
                m_consumer = null;
            }
        }
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
            exec.createDataContainer(createOutTableSpec(m_convertToJSON, m_appendRecordInfo)));
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
        long numOfPolledRec = 0;
        // map used for syncing
        final Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();

        // init the consumer
        initConsumer(offsetMap);

        // while there are more messages and we were able to reconnect to Kafka
        while (!done) {
            // check for user interrupted
            exec.checkCanceled();

            // poll the messages
            final ConsumerRecords<String, String> consumerRecords = m_consumer.poll(m_pollTimeout);
            //            setEndOffsets(consumerRecords.partitions());
            // push all messages to the output table
            if (consumerRecords.count() > 0) {
                Iterator<TopicPartition> partIter = consumerRecords.partitions().iterator();
                while (!done && partIter.hasNext()) {
                    final TopicPartition msgPartition = partIter.next();
                    for (ConsumerRecord<String, String> record : consumerRecords.records(msgPartition)) {
                        if (m_breakCond == BreakCondition.TIME && record.timestamp() > m_time) {
                            m_consumer.pause(Collections.singleton(msgPartition));
                            break;
                        }
                        // append the record to the output
                        output.push(convert(record));
                        // add the offset to the map
                        offsetMap.put(msgPartition, new OffsetAndMetadata(record.offset() + 1));
                        if (m_breakCond == BreakCondition.MSG_COUNT && ++numOfPolledRec == m_totNumMsgs) {
                            done = true;
                            break;
                        }
                    }
                }
            } else {
                // break the loop if no messages were polled
                done = true;
            }
        }
        // commit the offsets w.r.t. to the processed messages
        m_consumer.commitSync(offsetMap);
        // close the output
        output.close();
    }

    /**
     * Creates the output table spec.
     *
     * @param useJSON <code>True</code> if the messages are converted to {@link JSONCell}
     * @param appendRecordInfo <code>True</code> if columns have to be append to the table storing information about the
     *            record.
     * @return the output table spec
     */
    public static DataTableSpec createOutTableSpec(final boolean useJSON, final boolean appendRecordInfo) {
        final DataTableSpecCreator specCreator = new DataTableSpecCreator();
        final DataType type;
        if (useJSON) {
            type = JSONCell.TYPE;
        } else {
            type = StringCell.TYPE;
        }
        specCreator.addColumns(createColSpec("Message", type));
        if (appendRecordInfo) {
            specCreator.addColumns(RECORD_COL_SPECS);
        }
        return specCreator.createSpec();
    }

    /**
     * Creates a column with the given spec.
     *
     * @param colName the name of the column to be created
     * @param dataType the data type of the column to be created
     * @return the data column spec
     */
    private static DataColumnSpec createColSpec(final String colName, final DataType dataType) {
        return new DataColumnSpecCreator(colName, dataType).createSpec();
    }

    /**
     * Initializes the KnimeConsumer.
     *
     * @param offsetMap the current offsets that have to be committed if the partitions are revoked
     *
     * @return the {@link KafkaConsumer}
     */
    private KafkaConsumer<String, String> initConsumer(final Map<TopicPartition, OffsetAndMetadata> offsetMap)
        throws InvalidSettingsException {
        if (m_consumer != null) {
            return m_consumer;
        }
        // validate the connection before creating a consumer
        KafkaConnectionValidator.testConnection(m_connectionProps, m_conValTimeout);

        // initialize the consumer
        m_consumer = new KafkaConsumer<>(m_properties);

        // rebalance listener initializing the offset maps
        final ConsumerRebalanceListener rebalanceListener = new ConsumerRebalanceListener() {

            @Override
            public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
                m_consumer.commitSync(offsetMap);
                offsetMap.clear();
            }

            @Override
            public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
            }
        };

        if (m_isPattern) {
            m_consumer.subscribe(Pattern.compile(m_topics), rebalanceListener);
        } else {
            m_consumer.subscribe(Arrays.asList(m_topics.trim().split("\\s*,\\s*", -1)), rebalanceListener);
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
    private DataRow convert(final ConsumerRecord<String, String> record) throws IOException {
        DataCell msgCell;
        if (m_convertToJSON) {
            msgCell = JSONCellFactory.create(record.value(), true);
        } else {
            msgCell = new StringCell(record.value());
        }
        if (m_appendRecordInfo) {
            return new DefaultRow(RowKey.createRowKey(m_rowIdOffset++), msgCell//
                , StringCellFactory.create(record.topic())//
                , IntCellFactory.create(record.partition())//
                , LongCellFactory.create(record.offset())//
                , ZonedDateTimeCellFactory
                    .create(ZonedDateTime.ofInstant(Instant.ofEpochMilli(record.timestamp()), m_zoneId))//
            );
        }
        return new DefaultRow(RowKey.createRowKey(m_rowIdOffset++), msgCell);
    }

}
