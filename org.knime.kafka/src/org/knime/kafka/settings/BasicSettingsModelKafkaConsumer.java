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

import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelLong;
import org.knime.core.node.defaultnodesettings.SettingsModelLongBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.kafka.ui.KafkaModel;
import org.knime.kafka.ui.KafkaProperty;

/**
 * Settings model storing all the basic information to consume entries from Kafka.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 *
 */
public abstract class BasicSettingsModelKafkaConsumer extends AbstractClientIDSettingsModelKafka {

    /** Config key of the kafka consumer */
    private static final String CFG_KAFKA_CONSUMER = "kafka-consumer-settings";

    /** All Kafka properties concerning the conumser. */
    private static final List<KafkaProperty> CONSUMER_PROPERTIES =
        Helper4KafkaConfig.getProperties(ConsumerConfig.class);

    /** The settings model storing the group id. */
    private final SettingsModelString m_groupID = new SettingsModelString("groupID", "KNIME");

    /** The settings model storing the number of max empty polls. */
    private final SettingsModelLong m_maxEmptyPolls =
        new SettingsModelLongBounded("maxEmptyPolls", 3, -1, Long.MAX_VALUE);

    /** The settings model storing the topic pattern flag. */
    private final SettingsModelBoolean m_topicPattern = new SettingsModelBoolean("topicPattern", false);

    /** The settings model storing the convert to JSON flag. */
    private final SettingsModelBoolean m_convertToJSON = new SettingsModelBoolean("convertToJSON", false);

    /** The settings model storing the append topic column flag. */
    private final SettingsModelBoolean m_appendTopicColumn = new SettingsModelBoolean("appendTopicColumn", false);

    /** The settings model storing the topics / topic pattern. */
    private final SettingsModelString m_topicModel = new SettingsModelString("topic", "");

    /** The settings model storing the poll timeout. */
    private final SettingsModelLongBounded m_pollTimeout =
        new SettingsModelLongBounded("poll-timeout-ms", 1000, 0, Integer.MAX_VALUE);

    /**
     * Constructor.
     */
    protected BasicSettingsModelKafkaConsumer() {
        super(CONSUMER_PROPERTIES);
        addModel(m_topicModel);
        addModel(m_groupID);
        addModel(m_maxEmptyPolls);
        addModel(m_topicPattern);
        addModel(m_convertToJSON);
        addModel(m_appendTopicColumn);
        addModel(m_pollTimeout);
    }

    /**
     * Returns the settings model storing the group id.
     *
     * @return the group id settings model
     */
    public final SettingsModelString getGroupIDSettingsModel() {
        return m_groupID;
    }

    /**
     * Returns the settings model storing the max empty polls policy.
     *
     * @return the max empty polls settings model
     */
    public final SettingsModelLong getMaxEmptyPollsSettingsModel() {
        return m_maxEmptyPolls;
    }

    /**
     * Returns settings model storing the topic pattern.
     *
     * @return the topic pattern settings model
     */
    public final SettingsModelBoolean getTopicPatternSettingsModel() {
        return m_topicPattern;
    }

    /**
     * Returns the settings model storing the use topic pattern flag.
     *
     * @return the use topic pattern settings model
     */
    public final boolean useTopicPattern() {
        return m_topicPattern.getBooleanValue();
    }

    /**
     * Returns the settings model storing the convert to JSON flag.
     *
     * @return the convert to JSON settings model
     */
    public final SettingsModelBoolean getConvertToJSONSettingsModel() {
        return m_convertToJSON;
    }

    /**
     * Returns the settings model storing the append topic column flag.
     *
     * @return the append topic column setting model
     */
    public final SettingsModelBoolean getAppendTopicColumnSettingsModel() {
        return m_appendTopicColumn;
    }

    /**
     * Returns the settings model storing the topics / topic pattern.
     *
     * @return the topic settings model
     */
    public final SettingsModelString getTopicsSettingsModel() {
        return m_topicModel;
    }

    /**
     * Returns the settings model storing the poll timeout.
     *
     * @return the poll timeout model
     */
    public final SettingsModelLongBounded getPollTimeoutSettingsModel() {
        return m_pollTimeout;
    }

    /**
     * Returns the convert to JSON flag.
     *
     * @return the convert to JSON flag
     */
    public final boolean convertToJSON() {
        return m_convertToJSON.getBooleanValue();
    }

    /**
     * Returns the append topic column flag.
     *
     * @return the append topic column flag
     */
    public final boolean appendTopic() {
        return m_appendTopicColumn.getBooleanValue();
    }

    /**
     * Returns the number of max empty polls. A value of -1 represents unlimited polling/streaming.
     *
     * @return number of empty polls or -1 for unlimited
     */
    public final long getMaxEmptyPolls() {
        return m_maxEmptyPolls.getLongValue();
    }

    /**
     * Returns the topic / topic pattern.
     *
     * @return the topic / topic pattern
     */
    public final String getTopic() {
        return m_topicModel.getStringValue();
    }

    /**
     * Returns the poll timeout value.
     *
     * @return the poll timeout value
     */
    public final long getPollTimeout() {
        return m_pollTimeout.getLongValue();
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
        return props;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected final void setAndAddKafkaModel() {
        m_kafkaModel = new SettingsModelKafka(CFG_KAFKA_CONSUMER, new KafkaModel(getAdvancedModelProperties()), true);
        addModel(m_kafkaModel);
    }

    /**
     * Sets the blocked properties to the Kafka model.
     *
     * @param blockedProperties the blocked properties
     */
    @Override
    public final void setBlockedProps(final Set<String> blockedProperties) {
        m_kafkaModel.setBlockedProps(blockedProperties);
    }

}
