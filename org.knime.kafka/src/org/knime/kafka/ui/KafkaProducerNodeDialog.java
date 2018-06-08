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
package org.knime.kafka.ui;

import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.swing.AbstractButton;
import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.JPanel;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.core.data.StringValue;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentButtonGroup;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;
import org.knime.core.node.defaultnodesettings.DialogComponentNumberEdit;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.kafka.settings.SettingsModelKafkaProducer;
import org.knime.kafka.settings.SettingsModelKafkaProducer.TransactionCommitOption;

/**
 * The node dialog containing all relevant settings for the Kafka producer nodes.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 *
 */
public final class KafkaProducerNodeDialog extends AbstractKafkaClientIDDialog<SettingsModelKafkaProducer> {

    /** The transaction tab name. */
    private static final String TRANSACTION_TAB_NAME = "Transaction Settings";

    /** The transaction commit options title */
    private static final String TRANS_COMMIT_OPTIONS_TITLE = "Transaction Commit Options";

    /** The topics component label. */
    private static final String TOPICS_COMP_LABEL = "Topic(s)";

    /** The message column component label. */
    private static final String MESSAGE_COLUMN_COMP_LABEL = "Message column";

    /** The use transaction component label. */
    private static final String USE_TRANSACTIONS = "Use transactions";

    /** The transaction id component label. */
    private static final String TRANSACTION_ID = "Transaction ID";

    /** The topics tooltip text. */
    private static final String TOPICS_TOOLTIP = "<html>A comma-separated list of <tt>topics</tt></html>";

    /** The use transaction dialog component. */
    private DialogComponentBoolean m_useTrans;

    /** The transaction id dialog component. */
    private DialogComponentString m_transID;

    /** The transaction commit interval dialog component. */
    private DialogComponentNumberEdit m_transCommitInterval;

    /** The transaction commit option dialog component. */
    private DialogComponentButtonGroup m_transCommitOption;

    /**
     * Constructor.
     *
     * @param kafkaSettings an instance of {@link SettingsModelKafkaProducer}
     */
    public KafkaProducerNodeDialog(final SettingsModelKafkaProducer kafkaSettings) {
        super(kafkaSettings);
        addTabAt(1, TRANSACTION_TAB_NAME, createTransactionPanel());
        toggleTransaction();
        addListeners();
        registerDialogComponent(m_useTrans, m_transID, m_transCommitInterval, m_transCommitOption);
    }

    /**
     * @return
     */
    private JPanel createTransactionPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.LINE_START;
        gbc.weightx = 0;
        gbc.weighty = 0;
        gbc.gridwidth = 1;
        gbc.insets = new Insets(0, 5, 0, 0);
        gbc.fill = GridBagConstraints.NONE;
        gbc.gridy = 0;

        m_useTrans = new DialogComponentBoolean(getModel().getUseTransactionsSettingsModel(), USE_TRANSACTIONS);
        panel.add(m_useTrans.getComponentPanel(), gbc);
        ++gbc.gridy;

        m_transID = new DialogComponentString(getModel().getTransactionIdSettingsModel(), TRANSACTION_ID, true,
            DEFAULT_STRING_INPUT_COMP_WIDTH);
        panel.add(m_transID.getComponentPanel(), gbc);
        ++gbc.gridy;

        gbc.insets = new Insets(5, 5, 0, 0);
        panel.add(createCommitOptionsPanel(), gbc);

        // create component to ensure that the components are shown at the top
        // left corner
        ++gbc.gridy;
        gbc.weightx = 1;
        gbc.weighty = 1;
        gbc.fill = GridBagConstraints.BOTH;

        panel.add(Box.createGlue(), gbc);

        // return the panel
        return panel;
    }

    private JPanel createCommitOptionsPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        panel.setBorder(
            BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), TRANS_COMMIT_OPTIONS_TITLE));

        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.LINE_START;
        gbc.weightx = 1;
        gbc.weighty = 0;
        gbc.fill = GridBagConstraints.NONE;
        gbc.gridy = 0;
        gbc.gridx = 0;
        gbc.insets = new Insets(0, 5, 0, 0);

        m_transCommitOption =
            new DialogComponentButtonGroup(getModel().getTransactionCommitOptionSettingsModel(), true, null, //
                Arrays.stream(TransactionCommitOption.values())//
                    .map(opt -> opt.toString())//
                    .toArray(String[]::new)//
            );

        // add the commit option
        panel.add(m_transCommitOption.getButton(TransactionCommitOption.TABLE_END.toString()), gbc);

        ++gbc.gridy;
        panel.add(createBatchPanel(m_transCommitOption.getButton(TransactionCommitOption.INTERVAL.toString())), gbc);

        // return the panel
        return panel;
    }

    /**
     * Create the batch size dialog component to the right of the batch-size radio button.
     *
     * @param button the batch size radio button
     * @return the panel holding the button on the left and the number edit dialog component to the right
     */
    private Component createBatchPanel(final AbstractButton button) {
        final JPanel panel = new JPanel(new GridBagLayout());

        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.LINE_START;
        gbc.weightx = 1;
        gbc.weighty = 0;
        gbc.gridwidth = 1;
        gbc.fill = GridBagConstraints.NONE;
        gbc.gridy = 0;
        gbc.gridx = 0;

        // add the button to the left
        panel.add(button, gbc);

        // add the batch size to the right
        ++gbc.gridx;
        //        gbc.insets = new Insets(0, 0, 0, 0);
        m_transCommitInterval = new DialogComponentNumberEdit(getModel().getTransactionCommitIntervalSettingsModel(),
            null, DEFAULT_NUMBER_INPUT_COMP_WIDTH);
        panel.add(m_transCommitInterval.getComponentPanel(), gbc);
        return panel;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    protected List<Component> getSettingComponents() {
        // set the tooltip
        final DialogComponentString topics = new DialogComponentString(getModel().getTopicSettingsModel(),
            TOPICS_COMP_LABEL, true, DEFAULT_STRING_INPUT_COMP_WIDTH);
        topics.setToolTipText(TOPICS_TOOLTIP);

        final DialogComponent[] diaComps = new DialogComponent[]{ //
            topics//
            , new DialogComponentColumnNameSelection(getModel().getMessageColumnSettingsModel(),
                MESSAGE_COLUMN_COMP_LABEL, 0, true, StringValue.class)//
        };

        // register the components
        registerDialogComponent(diaComps);

        // append the additional dialog components and return the list
        final List<Component> comps = super.getSettingComponents();
        return Arrays.stream(diaComps)//
            .map(DialogComponent::getComponentPanel)//
            .collect(Collectors.toCollection(() -> comps));
    }

    /**
     * Adds the listeners that disable and enable the options.
     */
    private void addListeners() {
        m_transCommitOption.getModel().addChangeListener(new ChangeListener() {

            @Override
            public void stateChanged(final ChangeEvent e) {
                toggleTransactionOption();
            }

        });

        m_useTrans.getModel().addChangeListener(new ChangeListener() {

            @Override
            public void stateChanged(final ChangeEvent e) {
                toggleTransaction();
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        super.loadSettingsFrom(settings, specs);
        toggleTransaction();
    }

    private void toggleTransaction() {
        final boolean sel = m_useTrans.isSelected();
        m_transID.getModel().setEnabled(sel);
        m_transCommitOption.getModel().setEnabled(sel);
        toggleTransactionOption();
    }

    private void toggleTransactionOption() {
        if (!m_transCommitOption.getModel().isEnabled()) {
            m_transCommitInterval.getModel().setEnabled(false);
        } else if (((SettingsModelString)m_transCommitOption.getModel()).getStringValue()
            .equals(TransactionCommitOption.TABLE_END.toString())) {
            m_transCommitInterval.getModel().setEnabled(false);
        } else {
            m_transCommitInterval.getModel().setEnabled(true);
        }
    }

}
