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

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelLongBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.kafka.algorithms.KNIMEKafkaConsumer;
import org.knime.kafka.algorithms.KNIMEKafkaConsumer.BreakCondition;
import org.knime.kafka.ui.KafkaModel;
import org.knime.kafka.ui.KafkaProperty;
import org.knime.time.util.SettingsModelDateTime;

/**
 * Settings model storing all the relevant information to consume entries from Kafka.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 *
 */
public final class SettingsModelKafkaConsumer extends AbstractClientIDSettingsModelKafka {

    /** Config key of the kafka consumer */
    private static final String CFG_KAFKA_CONSUMER = "kafka-consumer-settings";

    /** All Kafka properties concerning the conumser. */
    private static final List<KafkaProperty> CONSUMER_PROPERTIES =
        Helper4KafkaConfig.getProperties(ConsumerConfig.class);

    /** The settings model storing the group id. */
    private final SettingsModelString m_groupID = new SettingsModelString("group-id", "KNIME");

    /** The settings model storing the topic pattern flag. */
    private final SettingsModelBoolean m_topicPattern = new SettingsModelBoolean("topic-pattern", false);

    /** The settings model storing the convert to JSON flag. */
    private final SettingsModelBoolean m_convertToJSON = new SettingsModelBoolean("convert-to-JSON", false);

    /** The settings model storing the append topic column flag. */
    private final SettingsModelBoolean m_appendTopicColumn = new SettingsModelBoolean("append-topic-column", false);

    /** The settings model storing the topics / topic pattern. */
    private final SettingsModelString m_topicModel = new SettingsModelString("topic", "");

    /** The settings model storing the poll timeout. */
    private final SettingsModelLongBounded m_pollTimeout =
        new SettingsModelLongBounded("poll-timeout-ms", 1000, 0, Integer.MAX_VALUE);

    /** The settings model storing the consumption break condition. */
    private final SettingsModelString m_breakCondition =
        new SettingsModelString("stop-criterion", KNIMEKafkaConsumer.BreakCondition.TIME.toString());

    /** The settings model storing the maximum number of requested messages per poll. */
    private final SettingsModelIntegerBounded m_maxReqMsgs =
        new SettingsModelIntegerBounded("max-msgs-per-poll", 100, 1, Integer.MAX_VALUE);

    /** The settings model storing the total number of requested messages. */
    private final SettingsModelLongBounded m_totNumPolledMsg =
        new SettingsModelLongBounded("total-num-polled-msgs", 500, 1, Integer.MAX_VALUE);

    /** The settings model storing the time up to which messages have to be polled. */
    private final SettingsModelDateTime m_time =
        new SettingsModelDateTime("latest-date-time", ZonedDateTime.now().withNano(0));

    /** The settings model storing the use current time flag. */
    private final SettingsModelBoolean m_useCurTime = new SettingsModelBoolean("use-cur-time", false);

    /**
     * Constructor.
     */
    public SettingsModelKafkaConsumer() {
        super(CONSUMER_PROPERTIES);
        addModel(m_topicModel, m_groupID, m_topicPattern, m_convertToJSON, m_appendTopicColumn, m_pollTimeout,
            m_breakCondition, m_maxReqMsgs, m_totNumPolledMsg, m_time, m_useCurTime);
        setAndAddKafkaModel();
    }

    /**
     * Returns the settings model storing the group id.
     *
     * @return the group id settings model
     */
    public SettingsModelString getGroupIDSettingsModel() {
        return m_groupID;
    }

    /**
     * Returns settings model storing the topic pattern.
     *
     * @return the topic pattern settings model
     */
    public SettingsModelBoolean getTopicPatternSettingsModel() {
        return m_topicPattern;
    }

    /**
     * Returns the settings model storing the use topic pattern flag.
     *
     * @return the use topic pattern settings model
     */
    public boolean useTopicPattern() {
        return m_topicPattern.getBooleanValue();
    }

    /**
     * Returns the settings model storing the convert to JSON flag.
     *
     * @return the convert to JSON settings model
     */
    public SettingsModelBoolean getConvertToJSONSettingsModel() {
        return m_convertToJSON;
    }

    /**
     * Returns the settings model storing the append topic column flag.
     *
     * @return the append topic column setting model
     */
    public SettingsModelBoolean getAppendTopicColumnSettingsModel() {
        return m_appendTopicColumn;
    }

    /**
     * Returns the settings model storing the topics / topic pattern.
     *
     * @return the topic settings model
     */
    public SettingsModelString getTopicsSettingsModel() {
        return m_topicModel;
    }

    /**
     * Returns the settings model storing the poll timeout.
     *
     * @return the poll timeout settings model
     */
    public SettingsModelLongBounded getPollTimeoutSettingsModel() {
        return m_pollTimeout;
    }

    /**
     * Returns the settings model storing the consumption break condition.
     *
     * @return the consumption break condition settings model
     */
    public SettingsModelString getBreakConditionSettingsModel() {
        return m_breakCondition;
    }

    /**
     * Returns the settings model storing the maximum number of requested messages per poll.
     *
     * @return the max number of requested messages per poll model
     */
    public SettingsModelIntegerBounded getMaxReqMsgsSettingsModel() {
        return m_maxReqMsgs;
    }

    /**
     * Returns the total number of messages to be polled settings model.
     *
     * @return the number of messages to be polled settings model
     */
    public SettingsModelLongBounded getTotalMsgSettingsModel() {
        return m_totNumPolledMsg;
    }

    /**
     * Returns the settings model storing the date up to which messages have to be consumed.
     *
     * @return the settings model storing the date
     */
    public SettingsModelDateTime getTimeSettingsModel() {
        return m_time;
    }

    /**
     * Returns the settings model storing the use current time flag.
     *
     * @return the settings model storing the use current time flag
     */
    public SettingsModelBoolean getUseCurTimeSettingsModel() {
        return m_useCurTime;
    }

    /**
     * Returns the convert to JSON flag.
     *
     * @return the convert to JSON flag
     */
    public boolean convertToJSON() {
        return m_convertToJSON.getBooleanValue();
    }

    /**
     * Returns the append topic column flag.
     *
     * @return the append topic column flag
     */
    public boolean appendTopic() {
        return m_appendTopicColumn.getBooleanValue();
    }

    /**
     * Returns the {@link BreakCondition}.
     *
     * @return the {@link BreakCondition}
     * @throws InvalidSettingsException - if the settings model storing the name has no corresponding
     *             {@link BreakCondition}
     */
    public BreakCondition getBreakCondition() throws InvalidSettingsException {
        return BreakCondition.getEnum(m_breakCondition.getStringValue());
    }

    /**
     * Returns the topic / topic pattern.
     *
     * @return the topic / topic pattern
     */
    public String getTopic() {
        return m_topicModel.getStringValue();
    }

    /**
     * Returns the poll timeout value.
     *
     * @return the poll timeout value
     */
    public long getPollTimeout() {
        return m_pollTimeout.getLongValue();
    }

    /**
     * Returns the number of messages to be polled.
     *
     * @return the number of messages to be polled
     */
    public long getTotNumMsgs() {
        return m_totNumPolledMsg.getLongValue();
    }

    /**
     * Returns the the time of the last messages that has to be polled.
     *
     * @return the point in time up to which messages ahve to polled
     */
    public long getTime() {
        final ZonedDateTime zDT;
        if (m_useCurTime.getBooleanValue()) {
            zDT = ZonedDateTime.now();
        } else {
            zDT = m_time.getZonedDateTime();
        }
        return zDT.toInstant().toEpochMilli();
    }

    /**
     * Returns the zone id.
     *
     * @return the zone id
     */
    public ZoneId getZone() {
        return m_time.getZone();

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Properties getBasicProperties() {
        final Properties props = super.getBasicProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, m_groupID.getStringValue());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, m_maxReqMsgs.getIntValue());
        return props;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void setAndAddKafkaModel() {
        m_kafkaModel = new SettingsModelKafka(CFG_KAFKA_CONSUMER, new KafkaModel(getAdvancedModelProperties()), true);
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

    /**
     * {@inheritDoc}
     */
    @Override
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.validateSettings(settings);
        final SettingsModelString tmp = m_breakCondition.createCloneWithValidatedValue(settings);
        tmp.loadSettingsFrom(settings);
        BreakCondition.getEnum(tmp.getStringValue());
    }

}
