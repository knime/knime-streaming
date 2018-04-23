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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.kafka.ui.KafkaModel;
import org.knime.kafka.ui.KafkaProperty;

/**
 * Settings model storing all the relevant information to connect to a Kafka broker.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 *
 */
public final class SettingsModelKafkaConnection extends AbstractSettingsModelKafka {

    /** Config key of the Kafka connection. */
    private static final String CFG_KAFKA_CONNECTION = "kafka-connection-settings";

    /** Config key for the server. */
    private static final String CFG_SERVER = "server";

    /** The default server string. */
    private static final String DEFAULT_SERVER = "";

    /** All Kafka properties concerning the connection, except the client id config. */
    private static final List<KafkaProperty> CONNECTION_PROPERTIES =
        Helper4KafkaConfig.getCommonProperties(ProducerConfig.class, ConsumerConfig.class,
            new HashSet<String>(Arrays.asList(new String[]{CommonClientConfigs.CLIENT_ID_CONFIG})));

    /** The server settings model. */
    private final SettingsModelString m_server = new SettingsModelString(CFG_SERVER, DEFAULT_SERVER);

    /**
     * Constructor.
     */
    public SettingsModelKafkaConnection() {
        super(CONNECTION_PROPERTIES);
        addModel(m_server);
        setAndAddKafkaModel();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Properties getBasicProperties() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, m_server.getStringValue());
        return props;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void setAndAddKafkaModel() {
        m_kafkaModel =
            new SettingsModelKafka(CFG_KAFKA_CONNECTION, new KafkaModel(getAdvancedModelProperties()), false);
        addModel(m_kafkaModel);
    }

    /**
     * Returns the servers string.
     *
     * @return the servers string
     */
    public String getServer() {
        return m_server.getStringValue();

    }

    /**
     * Returns the server settings model.
     *
     * @return the server settings model
     */
    public SettingsModelString getServerSettingsModel() {
        return m_server;
    }

}
