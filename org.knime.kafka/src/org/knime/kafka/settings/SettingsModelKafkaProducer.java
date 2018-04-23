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

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
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

    /** All Kafka properties concerning the producers. */
    private static final List<KafkaProperty> PRODUCER_PROPERTIES =
        Helper4KafkaConfig.getProperties(ProducerConfig.class);

    /** Config key for the Kafka producer. */
    private static final String CFG_KAFKA_PRODUCER = "kafka-producer-settings";

    /** The settings model storing the message column. */
    private final SettingsModelString m_col = new SettingsModelString("messageColumn", null);

    /** The settings model storing the topics. */
    private final SettingsModelString m_topics = new SettingsModelString("topics", "");

    /**
     * Constructor.
     */
    public SettingsModelKafkaProducer() {
        super(PRODUCER_PROPERTIES);
        addModel(m_col);
        addModel(m_topics);
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
     * {@inheritDoc}
     */
    @Override
    protected Properties getBasicProperties() {
        final Properties props = super.getBasicProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
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
