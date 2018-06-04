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
package org.knime.kafka.node.producer;

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
import org.knime.core.node.streamable.DataTableRowInput;
import org.knime.core.node.streamable.InputPortRole;
import org.knime.core.node.streamable.PartitionInfo;
import org.knime.core.node.streamable.PortInput;
import org.knime.core.node.streamable.PortOutput;
import org.knime.core.node.streamable.RowInput;
import org.knime.core.node.streamable.StreamableOperator;
import org.knime.kafka.algorithms.KNIMEKafkaProducer;
import org.knime.kafka.port.KafkaConnectorPortObject;
import org.knime.kafka.port.KafkaConnectorPortSpec;
import org.knime.kafka.settings.SettingsModelKafkaProducer;
import org.knime.kafka.settings.SettingsModelKafkaProducer.TransactionCommitOption;

/**
 * Node model for sending messages to Kafka.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 *
 */
final class KafkaProducerNodeModel extends NodeModel {

    /** The empty topics exception text. */
    private static final String EMPTY_TOPICS_EXCEPTION = "The <Topics> cannot be empty";

    /** The auto-configure prefix. */
    private static final String AUTO_CONFIGURE_PREFIX = "Message column preset to ";

    /** The missing message column exception. */
    private static final String MISSING_MSG_COL_EXCEPTION = "Please select a message column";

    /** The missing transaction id exception. */
    private static final String MISSING_TRANSACTION_ID = "The Transaction ID cannot be empty";

    /** The potentially missing transaction commit warning . */
    private static final String TRANSACTION_COMMIT_WARNING =
        "If this is an endless stream the transaction will never be committed";

    /** The Kafka producer settings model. */
    private SettingsModelKafkaProducer m_producerSettings;

    /** Constructor. */
    public KafkaProducerNodeModel() {
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
    public InputPortRole[] getInputPortRoles() {
        return new InputPortRole[]{InputPortRole.NONDISTRIBUTED_STREAMABLE, InputPortRole.NONDISTRIBUTED_NONSTREAMABLE};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final DataTableSpec tableSpec = (DataTableSpec)inSpecs[0];

        // initialize the settings models
        init();

        // auto-configure the message column
        String colName = m_producerSettings.getMessageColumn();
        if (colName == null) {
            for (final DataColumnSpec colSpec : tableSpec) {
                if (colSpec.getType().isCompatible(StringValue.class)) {
                    m_producerSettings.setMessageColumn(colSpec.getName());
                    setWarningMessage(AUTO_CONFIGURE_PREFIX + colSpec.getName());
                    break;
                }
            }
        }
        colName = m_producerSettings.getMessageColumn();
        if (colName == null) {
            throw new InvalidSettingsException(MISSING_MSG_COL_EXCEPTION);
        }
        if (!tableSpec.containsName(colName)) {
            throw new InvalidSettingsException("Column: " + colName + " not found in input table");
        }

        // check if the topics string is empty
        if (m_producerSettings.getTopics().isEmpty()) {
            throw new InvalidSettingsException(EMPTY_TOPICS_EXCEPTION);
        }

        // check if the transaction ID is set
        if (m_producerSettings.useTransactions()
            && (m_producerSettings.getTransactionID() == null || m_producerSettings.getTransactionID().isEmpty())) {
            throw new InvalidSettingsException(MISSING_TRANSACTION_ID);
        }

        // test if options are controlled via flow var
        TransactionCommitOption.getEnum(m_producerSettings.getTransactionCommitOptionSettingsModel().getStringValue());

        // set the blocked properties
        m_producerSettings.setBlockedProps(((KafkaConnectorPortSpec)inSpecs[1]).getBlockedProperties());

        return new PortObjectSpec[]{};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final KafkaConnectorPortObject port = ((KafkaConnectorPortObject)inObjects[1]);
        run(new DataTableRowInput((BufferedDataTable)inObjects[0]), port.getConnectionProperties(),
            port.getConnectionValidationTimeout(), exec);
        return new PortObject[]{};
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
                // set a warning if this node is executed in streaming mode but only commits transactions at the end
                // of its execution
                if (m_producerSettings.useTransactions() && m_producerSettings.getTransactionCommitInterval() <= 0) {
                    setWarningMessage(TRANSACTION_COMMIT_WARNING);
                }
                final RowInput input = (RowInput)inputs[0];
                KafkaConnectorPortSpec spec = ((KafkaConnectorPortSpec)inSpecs[1]);
                run(input, spec.getConnectionProperties(), spec.getConnectionValiditionTimeout(), exec);
                return;
            }
        };
    }

    /**
     * Runs the Kafka producer.
     *
     * @param input the row input
     * @param connectionProps the connection properties
     * @param conValTimeout the connection validation timeout
     * @param exec the execution context
     * @throws Exception - If the user canceled the execution or there are problems related to Kafka, i.e., missing
     *             connection or invalid settings
     */
    private void run(final RowInput input, final Properties connectionProps, final int conValTimeout,
        final ExecutionContext exec) throws Exception {
        final KNIMEKafkaProducer producer = new KNIMEKafkaProducer(connectionProps, m_producerSettings.getProperties(),
            m_producerSettings.getTopics(), m_producerSettings.getMessageColumn(), conValTimeout,
            m_producerSettings.useTransactions(), m_producerSettings.getTransactionCommitInterval());
        try {
            // execute the producer
            producer.execute(exec, input);
        } catch (final Exception e) {
            throw (e);
        } finally {
            // close the producer
            producer.close();
        }
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
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        init();
        m_producerSettings.loadSettingsFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        // nothing to reset
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
     * Creates the Kafka producer settings model.
     *
     * @return the Kafka producer settings model
     */
    static SettingsModelKafkaProducer createProducerSettingsModel() {
        return new SettingsModelKafkaProducer();
    }

}
