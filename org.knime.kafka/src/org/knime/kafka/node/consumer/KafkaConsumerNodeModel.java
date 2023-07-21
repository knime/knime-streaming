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
package org.knime.kafka.node.consumer;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.streamable.OutputPortRole;
import org.knime.core.node.streamable.PartitionInfo;
import org.knime.core.node.streamable.PortInput;
import org.knime.core.node.streamable.PortOutput;
import org.knime.core.node.streamable.RowOutput;
import org.knime.core.node.streamable.StreamableOperator;
import org.knime.kafka.algorithms.KNIMEKafkaConsumer;
import org.knime.kafka.algorithms.KNIMEKafkaConsumer.BreakCondition;
import org.knime.kafka.port.KafkaConnectorPortObject;
import org.knime.kafka.port.KafkaConnectorPortSpec;
import org.knime.kafka.settings.SettingsModelKafkaConsumer;

/**
 * Node model consuming messages from Kafka.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 *
 */
final class KafkaConsumerNodeModel extends NodeModel {

    /** The empty topics exception text. */
    private static final String EMPTY_TOPICS_EXCEPTION = "The <Topics> cannot be empty";

    /** The Kafka consumer settings model. */
    private SettingsModelKafkaConsumer m_consumerSettings;

    /** Constructor. */
    public KafkaConsumerNodeModel() {
        super(new PortType[]{KafkaConnectorPortObject.TYPE}, new PortType[]{BufferedDataTable.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OutputPortRole[] getOutputPortRoles() {
        return new OutputPortRole[]{OutputPortRole.DISTRIBUTED};
    }

    /**
     * Initializes all the settings models.
     */
    private void init() {
        if (m_consumerSettings == null) {
            m_consumerSettings = createConsumerSettingsModel();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        // init the consumer
        final KafkaConnectorPortObject port = ((KafkaConnectorPortObject)inObjects[0]);
        KNIMEKafkaConsumer consumer = null;
        consumer = getConsumer(port.getConnectionProperties(), m_consumerSettings.getProperties(),
            port.getConnectionValidationTimeout());
        try {
            // execute the consumer
            return new PortObject[]{consumer.execute(exec)};
        } finally {
            // ensure that the consumer is closed
            consumer.close();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamableOperator createStreamableOperator(final PartitionInfo partitionInfo,
        final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        return new StreamableOperator() {
            @Override
            public void runFinal(final PortInput[] inputs, final PortOutput[] outputs, final ExecutionContext exec)
                throws Exception {

                // init the consumer
                final KafkaConnectorPortSpec spec = (KafkaConnectorPortSpec)inSpecs[0];
                final KNIMEKafkaConsumer consumer = getConsumer(spec.getConnectionProperties(),
                    m_consumerSettings.getProperties(), spec.getConnectionValiditionTimeout());
                try {
                    // execute the consumer
                    consumer.execute(exec, (RowOutput)outputs[0]);
                } finally {
                    // ensure that the consumer is closed
                    consumer.close();
                }
            }
        };
    }

    /**
     * Creates the Kafka consumer using the provided settings.
     *
     * @param connectionProps the Kafka connection properties
     * @param consumerProps the Kafka consumer properties
     * @param conValTimeout the connection validation timeout
     * @return the properly intialized Kafka consumer
     */
    private KNIMEKafkaConsumer getConsumer(final Properties connectionProps, final Properties consumerProps,
        final int conValTimeout) throws InvalidConfigurationException {
        try {
            final BreakCondition breakCond = m_consumerSettings.getBreakCondition();

            final KNIMEKafkaConsumer.Builder builder;
            switch (breakCond) {
                case TIME:
                    builder =
                        KNIMEKafkaConsumer.getDateBuilder(connectionProps, consumerProps, m_consumerSettings.getTopic(),
                            m_consumerSettings.useTopicPattern(), conValTimeout, m_consumerSettings.getTime());
                    break;
                case MSG_COUNT:
                default:
                    builder = KNIMEKafkaConsumer.getMsgCountBuilder(connectionProps, consumerProps,
                        m_consumerSettings.getTopic(), m_consumerSettings.useTopicPattern(), conValTimeout,
                        m_consumerSettings.getTotNumMsgs());
                    break;
            }
            return builder//
                .appendRecordInfo(m_consumerSettings.appendTopic())//
                .convertToJSON(m_consumerSettings.convertToJSON())//
                .setRowIdOffset(0)//
                .setPollTimeout(m_consumerSettings.getPollTimeout())//
                .setZoneId(m_consumerSettings.getZone())//
                .build();
        } catch (final InvalidSettingsException e) {
            // cannot happen, since we check the correctness of the selected break condition
            // during validateSettings()
            throw new InvalidConfigurationException(e.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        init();
        m_consumerSettings.setBlockedProps(((KafkaConnectorPortSpec)inSpecs[0]).getBlockedProperties());

        // check if the topics string is empty
        if (m_consumerSettings.getTopic().isEmpty()) {
            throw new InvalidSettingsException(EMPTY_TOPICS_EXCEPTION);
        }

        // test if options are controlled via flow var
        BreakCondition.getEnum(m_consumerSettings.getBreakConditionSettingsModel().getStringValue());

        return new DataTableSpec[]{KNIMEKafkaConsumer.createOutTableSpec(m_consumerSettings.convertToJSON(),
            m_consumerSettings.appendTopic())};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        if (m_consumerSettings != null) {
            m_consumerSettings.saveSettingsTo(settings);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        init();
        m_consumerSettings.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        init();
        m_consumerSettings.loadSettingsFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // nothing to load
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // nothing to save
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        // nothing to reset
    }

    /**
     * Creates the Kafka loop start settings model.
     *
     * @return the Kafka loop start settings model
     */
    static SettingsModelKafkaConsumer createConsumerSettingsModel() {
        return new SettingsModelKafkaConsumer();
    }
}
