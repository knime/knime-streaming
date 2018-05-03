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
package org.knime.kafka.node.loops.start;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelLongBounded;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.workflow.LoopStartNodeTerminator;
import org.knime.kafka.algorithms.KNIMEKafkaConsumer;
import org.knime.kafka.port.KafkaConnectorPortObject;
import org.knime.kafka.port.KafkaConnectorPortSpec;
import org.knime.kafka.settings.SettingsModelKafkaLoopStart;

/**
 * Node model for consuming messages from Kafka in a looped scenario.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 *
 */
final class LoopStartKafkaNodeModel extends NodeModel implements LoopStartNodeTerminator {

    /** The empty topics exception text. */
    private static final String EMPTY_TOPICS_EXCEPTION = "The <Topics> cannot be empty";

    /** The current iteration name. */
    private static final String CURRENT_ITERATION_NAME = "currentIteration";

    /** Config key for the batch size. */
    private static final String CFG_BATCH = "batch-size";

    /** Default batch size. */
    private static final int DEFAULT_BATCH_SIZE = 10;

    /** The Kafka consumer settings model. */
    private SettingsModelKafkaLoopStart m_consumerSettings;

    /** The batch size settings model. */
    private SettingsModelLongBounded m_batchModel;

    /** The iteration count. */
    private int m_iterationCount = 0;

    /** <code>True</code> if the loop has terminated. */
    private boolean m_terminated = false;

    /** The KNIME Kafka consumer */
    private KNIMEKafkaConsumer m_kafkaConsumer;

    /**
     * Creates a new model.
     */
    public LoopStartKafkaNodeModel() {
        super(new PortType[]{KafkaConnectorPortObject.TYPE}, new PortType[]{BufferedDataTable.TYPE});
    }

    /**
     * Initializes all the setting models.
     */
    private void init() {
        if (m_consumerSettings == null) {
            m_consumerSettings = createConsumerSettingsModel();
        }
        if (m_batchModel == null) {
            m_batchModel = createBatchSizeModel();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (m_batchModel == null) {
            m_batchModel = createBatchSizeModel();
            setWarningMessage("Using default batch size: " + DEFAULT_BATCH_SIZE);
        }

        // init the remaining setting models
        init();

        // set the blocked properties
        m_consumerSettings.setBlockedProps(((KafkaConnectorPortSpec)inSpecs[0]).getBlockedProperties());

        if (m_consumerSettings.getTopic().isEmpty()) {
            throw new InvalidSettingsException(EMPTY_TOPICS_EXCEPTION);
        }

        // push flow variables
        pushFlowVariableInt(CURRENT_ITERATION_NAME, m_iterationCount++);

        // return the data spec
        return new DataTableSpec[]{KNIMEKafkaConsumer.createOutTableSpec(m_consumerSettings.convertToJSON(),
            m_consumerSettings.appendTopic())};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        // init the consumer
        final KafkaConnectorPortObject port = ((KafkaConnectorPortObject)inObjects[0]);
        initConsumer(port.getConnectionProperties(), m_consumerSettings.getProperties(),
            port.getConnectionValidationTimeout());
        BufferedDataTable outData = null;

        // execute the consumer, if this fails reset will be called
        outData = m_kafkaConsumer.execute(exec);

        // check for termination
        if (m_kafkaConsumer.isStarved()) {
            m_terminated = true;
            terminateConnection();
        }

        // push flow variables
        pushFlowVariableInt(CURRENT_ITERATION_NAME, m_iterationCount++);

        // return data
        return new BufferedDataTable[]{outData};

    }

    /**
     * Creates the Kafka consumer using the provided settings.
     *
     * @param connectionProps the Kafka connection properties
     * @param consumerProps the Kafka consumer properties
     * @param conValTimeout the connection validation timeout
     */
    private void initConsumer(final Properties connectionProps, final Properties consumerProps,
        final int conValTimeout) {
        if (m_kafkaConsumer == null) {
            m_kafkaConsumer =
                new KNIMEKafkaConsumer.Builder(connectionProps, consumerProps, m_consumerSettings.getTopic(),
                    m_consumerSettings.useTopicPattern(), m_consumerSettings.getMaxEmptyPolls(), conValTimeout)//
                        .appendTopic(m_consumerSettings.appendTopic())//
                        .convertToJSON(m_consumerSettings.convertToJSON())//
                        .setBatchSize(m_batchModel.getLongValue())//
                        .setOffset(0)//
                        .setPollTimeout(m_consumerSettings.getPollTimeout())//
                        .build();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        terminateConnection();
        m_iterationCount = 0;
        m_terminated = false;
    }

    /**
     * Terminates the connection if necessary.
     */
    private void terminateConnection() {
        if (m_kafkaConsumer != null) {
            m_kafkaConsumer.close();
            m_kafkaConsumer = null;
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean terminateLoop() {
        return m_terminated;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        if (m_batchModel != null) {
            m_batchModel.saveSettingsTo(settings);
        }
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
        m_batchModel.validateSettings(settings);
        m_consumerSettings.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        init();
        m_batchModel.loadSettingsFrom(settings);
        m_consumerSettings.loadSettingsFrom(settings);
    }

    /**
     * Creates the batch size settings model.
     *
     * @return the batch size settings model
     */
    static SettingsModelLongBounded createBatchSizeModel() {
        return new SettingsModelLongBounded(CFG_BATCH, DEFAULT_BATCH_SIZE, 1, Integer.MAX_VALUE);
    }

    /**
     * Creathes the Kafka consumer settings model.
     *
     * @return the Kafka consumer settings model
     */
    static SettingsModelKafkaLoopStart createConsumerSettingsModel() {
        return new SettingsModelKafkaLoopStart();
    }

}
