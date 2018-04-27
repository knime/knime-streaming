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
package org.knime.kafka.node.loops.end;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.StringValue;
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
import org.knime.core.node.workflow.LoopEndNode;
import org.knime.core.node.workflow.LoopStartNodeTerminator;
import org.knime.kafka.algorithms.KNIMEKafkaProducer;
import org.knime.kafka.port.KafkaConnectorPortObject;
import org.knime.kafka.port.KafkaConnectorPortSpec;
import org.knime.kafka.settings.SettingsModelKafkaProducer;

/**
 * Node model for producing Kafka messages in a loop scenario.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 *
 */
final class LoopEndKafkaNodeModel extends NodeModel implements LoopEndNode {

    /** The empty topics exception text. */
    private static final String EMPTY_TOPICS_EXCEPTION = "The <Topics> cannot be empty";

    /** The auto-configure prefix. */
    private static final String AUTO_CONFIGURE_PREFIX = "Message column preset to ";

    /** The loop end exception. */
    private static final String LOOP_CONNECTION_EXCEPTION =
        "Loop End is not connected to matching/corresponding Loop Start node.";

    /** The missing message column exception. */
    private static final String MISSING_MSG_COL_EXCEPTION = "Please select a message column";

    /** The Kafka producer settings model. */
    private SettingsModelKafkaProducer m_producerSettings;

    /** The KNIME Kafka producer. */
    private KNIMEKafkaProducer m_kafkaProduer;

    /** Constructor. */
    public LoopEndKafkaNodeModel() {
        super(new PortType[]{BufferedDataTable.TYPE, KafkaConnectorPortObject.TYPE}, new PortType[]{});
    }

    /**
     * Initializes the settings models.
     */
    private void init() {
        if (m_producerSettings == null) {
            m_producerSettings = createProducerSettingsModel();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final DataTableSpec tableSpec = (DataTableSpec)inSpecs[0];

        // initialize all setting models
        init();

        // check if the loop start node implements the proper interface
        if (!(this.getLoopStartNode() instanceof LoopStartNodeTerminator)) {
            throw new IllegalStateException(LOOP_CONNECTION_EXCEPTION);
        }

        // auto-configure the message column
        if (m_producerSettings.getMessageColumn() == null) {
            for (final DataColumnSpec colSpec : tableSpec) {
                if (colSpec.getType().isCompatible(StringValue.class)) {
                    m_producerSettings.setMessageColumn(colSpec.getName());
                    setWarningMessage(AUTO_CONFIGURE_PREFIX + m_producerSettings.getMessageColumn());
                    break;
                }
            }
        }

        // check for correctly initialized message column
        if (m_producerSettings.getMessageColumn() == null) {
            throw new InvalidSettingsException(MISSING_MSG_COL_EXCEPTION);
        }
        if (!tableSpec.containsName(m_producerSettings.getMessageColumn())) {
            throw new InvalidSettingsException(
                "Column: " + m_producerSettings.getMessageColumn() + " not found in input table");
        }

        // check if the topics string is empty
        if (m_producerSettings.getTopics().isEmpty()) {
            throw new InvalidSettingsException(EMPTY_TOPICS_EXCEPTION);
        }

        // set the blocked properties
        m_producerSettings.setBlockedProps(((KafkaConnectorPortSpec)inSpecs[1]).getBlockedProperties());

        return new PortObjectSpec[]{};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        // merge all properties required for the Kafka producer
        final KafkaConnectorPortObject port = ((KafkaConnectorPortObject)inObjects[1]);
        // init the producer
        initProducer(port.getConnectionProperties(), m_producerSettings.getProperties(),
            port.getConnectionValidationTimeout());

        // execute the producer, if this fails reset will be called
        m_kafkaProduer.execute(exec, (BufferedDataTable)inObjects[0]);

        // test if the loop start is terminated and proceed accordingly
        boolean terminateLoop = ((LoopStartNodeTerminator)this.getLoopStartNode()).terminateLoop();
        if (!terminateLoop) {
            continueLoop();
        } else {
            closeConnection();
        }
        return new PortObject[]{};
    }

    /**
     * Initializes the Kafka producer.
     *
     * @param connectionProps the connection properties
     * @param producerProps the producer properties
     * @param conValTimeout the connection validation timeout
     */
    private void initProducer(final Properties connectionProps, final Properties producerProps,
        final int conValTimeout) {
        if (m_kafkaProduer == null) {
            m_kafkaProduer = new KNIMEKafkaProducer(connectionProps, producerProps, m_producerSettings.getTopics(),
                m_producerSettings.getMessageColumn(), conValTimeout);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        init();
        m_producerSettings.loadSettingsFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        if (m_producerSettings != null) {
            m_producerSettings.saveSettingsTo(settings);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        init();
        m_producerSettings.validateSettings(settings);
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
        closeConnection();
    }

    private void closeConnection() {
        if (m_kafkaProduer != null) {
            m_kafkaProduer.close();
            m_kafkaProduer = null;
        }
    }

    /**
     * Creates the Kafka producer settings model.
     *
     * @return the Kafka producer settings model
     */
    static SettingsModelKafkaProducer createProducerSettingsModel() {
        return new SettingsModelKafkaProducer();
    }

}
