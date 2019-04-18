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
package org.knime.kafka.settings;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.kafka.ui.KafkaModel;
import org.knime.kafka.ui.KafkaProperty;

/**
 * Settings model storing all the relevant information to produce entries for Kafka.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 *
 */
public final class SettingsModelKafkaProducer extends AbstractClientIDSettingsModelKafka {

    /**
     * Enum specifying the transaction commit options.
     *
     * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
     */
    public enum TransactionCommitOption {

            /** Indicates that the transaction has to be committed at the end of the input. */
            TABLE_END("Input end"),

            /** Indicates that the transaction has to be committed after a batch of rows. */
            INTERVAL("Batchwise");

        /** Missing name exception. */
        private static final String NAME_MUST_NOT_BE_NULL = "Name must not be null";

        /** IllegalArgumentException prefix. */
        private static final String ARGUMENT_EXCEPTION_PREFIX = "No TransactionCommitOption constant with name: ";

        private final String m_name;

        /**
         * Constructor
         *
         * @param name the enum name
         */
        private TransactionCommitOption(final String name) {
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
         * @throws InvalidSettingsException if the given name is not associated with an {@link TransactionCommitOption}
         *             value
         */
        public static TransactionCommitOption getEnum(final String name) throws InvalidSettingsException {
            if (name == null) {
                throw new InvalidSettingsException(NAME_MUST_NOT_BE_NULL);
            }
            return Arrays.stream(values()).filter(t -> t.m_name.equals(name)).findFirst()
                .orElseThrow(() -> new InvalidSettingsException(ARGUMENT_EXCEPTION_PREFIX + name));
        }
    }

    /**
     * Enum specifying how the producer should deal with records send to the Kafka broker.
     *
     * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
     */
    public enum SendingType {

            /** Sends records asynchronously to the Kafka broker. */
            ASYNCHRONOUS("Asynchronous"),

            /** Sends records synchronously to the Kafka broker. */
            SYCHRONOUS("Synchronous"),

            /** Sends records ignoring whether or not the Kafka broker receives them. */
            FIRE_FORGET("Fire and forget");

        /** Missing name exception. */
        private static final String NAME_MUST_NOT_BE_NULL = "Name must not be null";

        /** IllegalArgumentException prefix. */
        private static final String ARGUMENT_EXCEPTION_PREFIX = "No SendingType constant with name: ";

        /** The options UI name. */
        private final String m_name;

        private SendingType(final String name) {
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
         * @throws InvalidSettingsException if the given name is not associated with an {@link SendingType} value
         */
        public static SendingType getEnum(final String name) throws InvalidSettingsException {
            if (name == null) {
                throw new InvalidSettingsException(NAME_MUST_NOT_BE_NULL);
            }
            return Arrays.stream(values()).filter(t -> t.m_name.equals(name)).findFirst()
                .orElseThrow(() -> new InvalidSettingsException(ARGUMENT_EXCEPTION_PREFIX + name));
        }

    }

    /** All Kafka properties concerning the producers. */
    private static final List<KafkaProperty> PRODUCER_PROPERTIES = Helper4KafkaConfig.getProperties(
        ProducerConfig.class, new HashSet<String>(Arrays.asList(new String[]{ProducerConfig.TRANSACTIONAL_ID_CONFIG})));

    /** Config key for the Kafka producer. */
    private static final String CFG_KAFKA_PRODUCER = "kafka-producer-settings";

    /** The settings model storing the message column. */
    private final SettingsModelString m_col = new SettingsModelString("message-column", null);

    /** The settings model storing the topics. */
    private final SettingsModelString m_topics = new SettingsModelString("topics", "");

    /** The settings model storing the transaction id. */
    private final SettingsModelString m_transID = new SettingsModelString("transaction-id", "");

    /** The settings model storing the transaction commit interval. */
    private final SettingsModelInteger m_transCommitInterval =
        new SettingsModelIntegerBounded("batch-size", 50, 1, Integer.MAX_VALUE);

    /** The settings model storing the use transaction flag. */
    private final SettingsModelBoolean m_useTrans = new SettingsModelBoolean("use-transactions", false);

    /** The settings model storing the transaction commit option. */
    private final SettingsModelString m_transCommitOption =
        new SettingsModelString("transaction-commit-option", TransactionCommitOption.TABLE_END.toString());

    /** The settings model storing the send option. */
    private final SettingsModelString m_sendingOption =
        new SettingsModelString("sending-option", SendingType.ASYNCHRONOUS.toString()) {

            @Override
            protected void validateSettingsForModel(final NodeSettingsRO settings) throws InvalidSettingsException {
                if (settings.containsKey(getKey())) {
                    super.validateSettingsForModel(settings);
                }
            }

            @Override
            protected void loadSettingsForModel(final NodeSettingsRO settings) {
                setStringValue(settings.getString(getKey(), SendingType.SYCHRONOUS.toString()));
            }

        };

    /**
     * Constructor.
     */
    public SettingsModelKafkaProducer() {
        super(PRODUCER_PROPERTIES);
        addModel(m_col, m_topics, m_transID, m_transCommitInterval, m_useTrans, m_transCommitOption, m_sendingOption);
        setAndAddKafkaModel();
    }

    /**
     * Returns the message column name.
     *
     * @return the message column name
     */
    public String getMessageColumn() {
        return m_col.getStringValue();
    }

    /**
     * Sets the message column name.
     *
     * @param name the message column name
     */
    public void setMessageColumn(final String name) {
        m_col.setStringValue(name);
    }

    /**
     * Returns the topics.
     *
     * @return the topics
     */
    public String getTopics() {
        return m_topics.getStringValue();
    }

    /**
     * Returns <code>True</code> if the producer has to be executed in transaction mode.
     *
     * @return <code>True</code> if transactional execution is required
     */
    public boolean useTransactions() {
        return m_useTrans.getBooleanValue();
    }

    /**
     * Returns the transaction id.
     *
     * @return the transaction id
     */
    public String getTransactionID() {
        return m_transID.getStringValue();
    }

    /**
     * Returns the transaction commit interval.
     *
     * @return the transaction commit interval
     *
     */
    public int getTransactionCommitInterval() {
        if (m_transCommitOption.getStringValue().equals(TransactionCommitOption.TABLE_END.toString())) {
            return -1;
        }
        return m_transCommitInterval.getIntValue();
    }

    /**
     * Returns the stored {@link SendingType}.
     *
     * @return the set {@code SendingType}
     */
    public SendingType getSendingType() {
        try {
            return SendingType.getEnum(m_sendingOption.getStringValue());
        } catch (InvalidSettingsException ex) {
            return SendingType.ASYNCHRONOUS;
        }
    }

    /**
     * Returns the settings model storing the topics
     *
     * @return the topics settings model
     */
    public SettingsModelString getTopicSettingsModel() {
        return m_topics;
    }

    /**
     * Returns the settings model storing the message column name.
     *
     * @return the message column settings model
     */
    public SettingsModelString getMessageColumnSettingsModel() {
        return m_col;
    }

    /**
     * Returns the settings model storing the flag indicating if the producer is executed in transaction mode.
     *
     * @return the use transactions settings model
     */
    public SettingsModelBoolean getUseTransactionsSettingsModel() {
        return m_useTrans;
    }

    /**
     * Returns the settings model storing the transaction id.
     *
     * @return the transaction id settings model
     */
    public SettingsModelString getTransactionIdSettingsModel() {
        return m_transID;
    }

    /**
     * Returns the settings model storing the transaction commit interval.
     *
     * @return the transaction commit interval settings model
     */
    public SettingsModelInteger getTransactionCommitIntervalSettingsModel() {
        return m_transCommitInterval;
    }

    /**
     * Returns the settings model storing the transaction commit option.
     *
     * @return the transaction commit option settings model
     */
    public SettingsModelString getTransactionCommitOptionSettingsModel() {
        return m_transCommitOption;
    }

    /**
     * Returns the settings model storing the send option.
     *
     * @return the send option settings model
     */
    public SettingsModelString getSendOptionSettingsModel() {
        return m_sendingOption;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Properties getBasicProperties() {
        final Properties props = super.getBasicProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // setting this property to null or empty string is not allowed by Kafka, hence we only
        // set it if required and blacklist the entry (see PRODUCER_PROPERTIES constructor)
        if (m_useTrans.getBooleanValue()) {
            props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, m_transID.getStringValue());
        }
        return props;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void setAndAddKafkaModel() {
        m_kafkaModel = new SettingsModelKafka(CFG_KAFKA_PRODUCER, new KafkaModel(getAdvancedModelProperties()), true);
        addModel(m_kafkaModel);
    }

    /**
     * Sets the blocked properties to the Kafka model.
     *
     * @param blockedProperties the blocked properties
     */
    @Override
    public void setBlockedProps(final Set<String> blockedProperties) {
        m_kafkaModel.setBlockedProps(blockedProperties);
    }

}
