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
package org.knime.kafka.node.connector;

import java.io.File;
import java.io.IOException;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.kafka.algorithms.KafkaConnectionValidator;
import org.knime.kafka.port.KafkaConnectorPortObject;
import org.knime.kafka.settings.SettingsModelKafkaConnection;

/**
 * Node model storing all the relevant properties to create a connection to Kafka.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 *
 */
final class KafkaConnectorNodeModel extends NodeModel {

    /** The empty sever list exception text. */
    private static final String EMPTY_SERVER_EXCEPTION = "The <Server list> cannot be empty";

    /** The connection test timeout exception. */
    private static final String REQUEST_TIMEOUT_EXCEPTION = "The connection test timeout has to be greater than 0";

    /** Config key for the connection validation timeout settings model. */
    private static final String CFG_VALIDATION_TIMEOUT = "connection-test-timeout";

    /** The Kafka connection settings model. */
    private SettingsModelKafkaConnection m_conSettings;

    /** The connection validation timeout settings model. */
    private SettingsModelIntegerBounded m_conValidationTimeoutSettings;

    /**
     * Constructor.
     */
    KafkaConnectorNodeModel() {
        super(new PortType[]{}, new PortType[]{KafkaConnectorPortObject.TYPE});
    }

    /**
     * Initializes the setting models.
     */
    void init() {
        if (m_conSettings == null) {
            m_conSettings = createConnectionSettingsModel();
        }
        if (m_conValidationTimeoutSettings == null) {
            m_conValidationTimeoutSettings = createConnectionValidationTimeoutModel();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        KafkaConnectionValidator.validateConnection(m_conSettings.getProperties(),
            m_conValidationTimeoutSettings.getIntValue());
        return new PortObject[]{new KafkaConnectorPortObject(m_conSettings.getProperties(),
            m_conSettings.getUsedProperties(), m_conValidationTimeoutSettings.getIntValue())};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        init();

        // check if the connection validation timeout is valid
        if (m_conValidationTimeoutSettings.getIntValue() <= 0) {
            throw new InvalidSettingsException(REQUEST_TIMEOUT_EXCEPTION);
        }

        // check if the server list is empty
        if (m_conSettings.getServer().isEmpty()) {
            throw new InvalidSettingsException(EMPTY_SERVER_EXCEPTION);
        }

        return new PortObjectSpec[]{KafkaConnectorPortObject.getSpec(m_conSettings.getProperties(),
            m_conSettings.getUsedProperties(), m_conValidationTimeoutSettings.getIntValue())};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        if (m_conSettings != null) {
            m_conSettings.saveSettingsTo(settings);
        }
        if (m_conValidationTimeoutSettings != null) {
            m_conValidationTimeoutSettings.saveSettingsTo(settings);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        init();
        m_conSettings.validateSettings(settings);
        m_conValidationTimeoutSettings.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        init();
        m_conSettings.loadSettingsFrom(settings);
        m_conValidationTimeoutSettings.loadSettingsFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        return;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        return;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        return;
    }

    /**
     * Creates the Kafka connection settings model.
     *
     * @return the Kafka connection settings model
     */
    static SettingsModelKafkaConnection createConnectionSettingsModel() {
        return new SettingsModelKafkaConnection();
    }

    /**
     * Creates the connection validation timeout settings model.
     *
     * @return the connection validation timeout settings model
     */
    static SettingsModelIntegerBounded createConnectionValidationTimeoutModel() {
        return new SettingsModelIntegerBounded(CFG_VALIDATION_TIMEOUT, 1000, 500, 60000);
    }

}
