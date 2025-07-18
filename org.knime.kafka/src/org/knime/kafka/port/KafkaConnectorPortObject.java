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

import java.util.Properties;
import java.util.Set;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.ModelContentWO;
import org.knime.core.node.port.AbstractSimplePortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.PortTypeRegistry;

/**
 * Port object that is passed to any node requiring to establish a connection to Kafka.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 *
 */
public final class KafkaConnectorPortObject extends AbstractSimplePortObject {

    /** Convenience accessor for the port type. */
    @SuppressWarnings("hiding")
    public static final PortType TYPE = PortTypeRegistry.getInstance().getPortType(KafkaConnectorPortObject.class);

    /** @noreference This class is not intended to be referenced by clients. */
    public static final class Serializer extends AbstractSimplePortObjectSerializer<KafkaConnectorPortObject> {
    }

    /** The summary text. */
    private static final String SUMMARY = "Port object holding the Kafka connection settings";

    /** The connector port spec. */
    private KafkaConnectorPortSpec m_portSpec;

    /** Empty constructor required by super class, should not be used. */
    public KafkaConnectorPortObject() {
        m_portSpec = null;
    }

    /**
     * Constructor.
     *
     * @param connectionProps the connection properties
     * @param blockedProps the blocked properties
     * @param conValTimeout the connection validation timeout (ms)
     */
    public KafkaConnectorPortObject(final Properties connectionProps, final Set<String> blockedProps,
        final int conValTimeout) {
        m_portSpec = new KafkaConnectorPortSpec(connectionProps, blockedProps, conValTimeout);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSummary() {
        return SUMMARY;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PortObjectSpec getSpec() {
        return m_portSpec;
    }

    /**
     * Returns the {@link KafkaConnectorPortSpec} spec.
     *
     * @param connectionProps the connection properties
     * @param blockedProps the blocked properties
     * @param conValTimeout the connection validation timeout (ms)
     * @return the {@link KafkaConnectorPortSpec} for the given parameters
     */
    public static KafkaConnectorPortSpec getSpec(final Properties connectionProps, final Set<String> blockedProps,
        final int conValTimeout) {
        return new KafkaConnectorPortSpec(connectionProps, blockedProps, conValTimeout);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void save(final ModelContentWO model, final ExecutionMonitor exec) throws CanceledExecutionException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void load(final ModelContentRO model, final PortObjectSpec spec, final ExecutionMonitor exec)
        throws InvalidSettingsException, CanceledExecutionException {
        m_portSpec = (KafkaConnectorPortSpec)spec;
    }

    /**
     * Convenience method to acess the connection properties stored in the {@link KafkaConnectorPortSpec}.
     *
     * @return the connection properties
     */
    public Properties getConnectionProperties() {
        return m_portSpec.getConnectionProperties();
    }

    /**
     * Returns the connection test timeout.
     *
     * @return the connection test timeout
     */
    public int getConnectionValidationTimeout() {
        return m_portSpec.getConnectionValiditionTimeout();
    }
}
