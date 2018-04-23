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
package org.knime.kafka.port;

import java.util.Arrays;
import java.util.Enumeration;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.ModelContentWO;
import org.knime.core.node.config.base.ConfigStringEntry;
import org.knime.core.node.port.AbstractSimplePortObjectSpec;

/**
 * Kafka connector port spec holding all (blocked) properties regarding the connection.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 *
 */
public final class KafkaConnectorPortSpec extends AbstractSimplePortObjectSpec {

    /** Config key for the blocked properties. */
    private static final String CFG_BLOCKED = "blocked-props";

    /** Config key for the connection properties. */
    private static final String CFG_MODEL_CONNECTION = "connection-props";

    private static final String CFG_VALIDTION_TIMEOUT = "connection-validation-timeout";

    /** The connection properties. */
    private Properties m_connectionProps;

    /** The blocked properties. */
    private Set<String> m_blockedProps;

    /** The connection validation timeout (ms). */
    private int m_conValTimeout;

    /**
     * @noreference This class is not intended to be referenced by clients.
     */
    public static final class Serializer extends AbstractSimplePortObjectSpecSerializer<KafkaConnectorPortSpec> {
    }

    /**
     * @noreference This class is not intended to be referenced by clients.
     */
    public KafkaConnectorPortSpec() {
    }

    /**
     * Constructor.
     *
     * @param connectionProps the connection properties
     * @param blockedProps the blocked properties
     * @param conValTimeout the connection validation timeout (ms)
     */
    KafkaConnectorPortSpec(final Properties connectionProps, final Set<String> blockedProps, final int conValTimeout) {
        m_connectionProps = connectionProps;
        m_blockedProps = blockedProps;
        m_conValTimeout = conValTimeout;
    }

    /**
     * Loader.
     *
     * @param model the model content
     * @return the properly initialized {@link KafkaConnectorPortSpec} instance.
     * @throws InvalidSettingsException
     */
    static KafkaConnectorPortSpec loadSpec(final ModelContentRO model) throws InvalidSettingsException {
        KafkaConnectorPortSpec spec = new KafkaConnectorPortSpec();
        spec.load(model);
        return spec;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void save(final ModelContentWO model) {
        model.addInt(CFG_VALIDTION_TIMEOUT, m_conValTimeout);
        model.addStringArray(CFG_BLOCKED, m_blockedProps.toArray(new String[m_blockedProps.size()]));
        saveProperties(model.addModelContent(CFG_MODEL_CONNECTION), m_connectionProps);
    }

    /**
     * Saves the properties to the model content.
     *
     * @param model the model content
     * @param properties the properties to be stored
     */
    private void saveProperties(final ModelContentWO model, final Properties properties) {
        for (Entry<Object, Object> entry : m_connectionProps.entrySet()) {
            model.addString((String)entry.getKey(), (String)entry.getValue());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void load(final ModelContentRO model) throws InvalidSettingsException {
        m_conValTimeout = model.getInt(CFG_VALIDTION_TIMEOUT);
        m_blockedProps = Arrays.stream(model.getStringArray(CFG_BLOCKED))//
            .collect(Collectors.toSet());
        loadProperties(model.getModelContent(CFG_MODEL_CONNECTION));
    }

    /**
     * Loads the properties.
     *
     * @param model the model content storing the properties
     * @throws InvalidSettingsException - If settings are incomplete/deficient.
     */
    private void loadProperties(final ModelContentRO model) throws InvalidSettingsException {
        m_connectionProps = new Properties();
        // save since we use the same schema to save the properties
        @SuppressWarnings("unchecked")
        Enumeration<ConfigStringEntry> entries = model.children();
        while (entries.hasMoreElements()) {
            final ConfigStringEntry entry = entries.nextElement();
            m_connectionProps.put(entry.getKey(), entry.getString());
        }
    }

    /**
     * Returns the connection properties.
     *
     * @return the connection properties
     */
    public Properties getConnectionProperties() {
        final Properties props = new Properties();
        props.putAll(m_connectionProps);
        return props;
    }

    /**
     * Returns the blocked properties.
     *
     * @return the blocked properties
     */
    public Set<String> getBlockedProperties() {
        return m_blockedProps;
    }

    /**
     * Returns the connection validation timeout.
     *
     * @return the connection validation timeout
     */
    public int getConnectionValiditionTimeout() {
        return m_conValTimeout;
    }

}
