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

import java.util.Enumeration;
import java.util.Properties;
import java.util.Set;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.config.base.ConfigStringEntry;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.kafka.algorithms.KNIMEKafkaConsumer;
import org.knime.kafka.algorithms.KNIMEKafkaProducer;
import org.knime.kafka.port.KafkaConnectorPortSpec;
import org.knime.kafka.ui.KafkaModel;

/**
 * Settings Model storing all information required to connect to Kafka and use {@link KNIMEKafkaConsumer} and
 * {@link KNIMEKafkaProducer}.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 *
 */
public final class SettingsModelKafka extends SettingsModel {

    /** The model type id. */
    private static final String MODEL_TYPE_ID = "SMID_kafka";

    /** The port spec exception message. */
    private static final String PORT_SPEC_EXCEPTION =
        "Missing port spec of type " + KafkaConnectorPortSpec.class.getSimpleName();

    /** The config name. */
    private final String m_configName;

    /** The Kafka model. */
    private final KafkaModel m_model;

    /** Flag indicating whether a KafkaConnectorPortSpec is required or not. */
    private boolean m_requiresSpec;

    /**
     * Constructor.
     *
     * @param configName the config name
     * @param model the {@link KafkaModel}
     * @param requiresSpec flag indicating whether an {@link KafkaConnectorPortSpec} is required or not
     */
    public SettingsModelKafka(final String configName, final KafkaModel model, final boolean requiresSpec) {
        m_configName = configName;
        m_requiresSpec = requiresSpec;
        m_model = model;
    }

    /**
     * Returns the Kafka model.
     *
     * @return the Kafka model
     */
    public KafkaModel getModel() {
        return m_model;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    protected SettingsModelKafka createClone() {
        return new SettingsModelKafka(m_configName, m_model.createClone(), m_requiresSpec);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getModelTypeID() {
        return MODEL_TYPE_ID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getConfigName() {
        return m_configName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsForDialog(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        // check if the port exists if it is required
        checkPorts(specs);

        // if an input port is required, access it and use it to block
        // properties
        if (m_requiresSpec) {
            for (final PortObjectSpec spec : specs) {
                if (spec instanceof KafkaConnectorPortSpec) {
                    m_model.blockProperties(((KafkaConnectorPortSpec)spec).getBlockedProperties());
                }
            }
        }
        // load the settings
        try {
            loadSettingsForModel(settings);
        } catch (InvalidSettingsException e) {
            throw (new NotConfigurableException(e.getMessage()));
        }
    }

    /**
     * Validates the model.
     *
     * @throws InvalidSettingsException - If the model is not valid
     */
    public void validate() throws InvalidSettingsException {
        m_model.validate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsForDialog(final NodeSettingsWO settings) throws InvalidSettingsException {
        validate();
        saveSettingsForModel(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettingsForModel(final NodeSettingsRO settings) throws InvalidSettingsException {
        // save since we use the same schema to save the rows
        @SuppressWarnings("unchecked")
        final Enumeration<ConfigStringEntry> rows = (Enumeration<ConfigStringEntry>)settings.getNodeSettings(getConfigName()).children();
        while (rows.hasMoreElements()) {
            final ConfigStringEntry row = rows.nextElement();
            m_model.validate(new String[]{row.getKey(), row.getString()});
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsForModel(final NodeSettingsRO settings) throws InvalidSettingsException {
        // clear the model
        m_model.clear();
        // save since we use the same schema to save the rows
        if (settings.containsKey(getConfigName())) {
            @SuppressWarnings("unchecked")
            final Enumeration<ConfigStringEntry> rows = (Enumeration<ConfigStringEntry>)settings.getNodeSettings(getConfigName()).children();
            while (rows.hasMoreElements()) {
                final ConfigStringEntry row = rows.nextElement();
                m_model.addRow(new String[]{row.getKey(), row.getString()});
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsForModel(final NodeSettingsWO settings) {
        final NodeSettingsWO rowsCont = settings.addNodeSettings(getConfigName());
        for (final String[] row : m_model.getData()) {
            rowsCont.addString(row[KafkaModel.KEY_IDX], row[KafkaModel.VAL_IDX]);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return getClass().getSimpleName() + " ('" + m_configName + "')";
    }

    /**
     * Calling the method causes the UI component associated with this model to update.
     */
    public void updateComponent() {
        m_model.fireTableDataChanged();
    }

    /**
     * Checks if the required port spec is available
     *
     * @param specs the available port specs
     * @throws NotConfigurableException - A port spec is required but none are provided, or none of the provided specs
     *             matches the required
     */
    public void checkPorts(final PortObjectSpec[] specs) throws NotConfigurableException {
        if (m_requiresSpec) {
            if (specs == null) {
                throw new NotConfigurableException(PORT_SPEC_EXCEPTION);
            }
            boolean containsSpec = false;
            for (final PortObjectSpec spec : specs) {
                if (spec != null && spec instanceof KafkaConnectorPortSpec) {
                    containsSpec = true;
                    break;
                }
            }
            if (!containsSpec) {
                throw new NotConfigurableException(PORT_SPEC_EXCEPTION);
            }
        }
    }

    /**
     * Return the properties.
     *
     * @return the properties
     */
    public Properties getProperties() {
        return m_model.getProperties();
    }

    /**
     * Assigns the blocked properties to the model.
     *
     * @param blockedProperties the blocked properties
     */
    public void setBlockedProps(final Set<String> blockedProperties) {
        m_model.blockProperties(blockedProperties);
    }
}
