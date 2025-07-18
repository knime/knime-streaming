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

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.ScrollPaneConstants;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;

/**
 * Kafka table panel allowing to add and remove rows to the KafkaJTable.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 *
 */
final class KafkaTablePanel extends JPanel {

    /** The logger. */
    private static final NodeLogger LOGGER = NodeLogger.getLogger(KafkaTablePanel.class);

    /** The default uid. */
    private static final long serialVersionUID = 1L;

    /** Default text for the add button. */
    private static final String ADD_BUTTON_TEXT = "Add";

    /** Default text for the add all button. */
    private static final String ADD_ALL_BUTTON_TEXT = "Add All";

    /** Default text for the remove button. */
    private static final String REMOVE_BUTTON_TEXT = "Remove";

    /** Default text for the remove all button. */
    private static final String REMOVE_ALL_BUTTON_TEXT = "Remove All";

    /** the underlying KafkaJTable component. */
    private final KafkaJTable m_kafkaTable;

    /** the add button. */
    private final JButton m_addButton = new JButton(ADD_BUTTON_TEXT);

    /** the add all button. */
    private final JButton m_addAllButton = new JButton(ADD_ALL_BUTTON_TEXT);

    /** the remove button. */
    private final JButton m_removeButton = new JButton(REMOVE_BUTTON_TEXT);

    /** the remove all button. */
    private final JButton m_removeAllButton = new JButton(REMOVE_ALL_BUTTON_TEXT);

    /**
     * Constructor.
     *
     * @param dm the underlying model
     */
    KafkaTablePanel(final KafkaModel dm) {

        // init the table and put it into a scroll pane
        m_kafkaTable = new KafkaJTable(dm);

        // add the button listeners
        addButtonListeners();

        // create the panel
        addComponents();

        // toggle the buttons
        toggleButtons();

    }

    /**
     * Adds the listeners to the different buttons.
     */
    private void addButtonListeners() {
        m_addButton.addActionListener(new ActionListener() {

            @Override
            public void actionPerformed(final ActionEvent e) {
                onAdd();
            }
        });

        m_addAllButton.addActionListener(new ActionListener() {

            @Override
            public void actionPerformed(final ActionEvent e) {
                onAddAll();
            }
        });

        m_removeButton.addActionListener(new ActionListener() {

            @Override
            public void actionPerformed(final ActionEvent e) {
                onRemove();
            }
        });

        m_removeAllButton.addActionListener(new ActionListener() {

            @Override
            public void actionPerformed(final ActionEvent e) {
                onRemoveAll();
            }
        });
    }

    /**
     * Add all components to the panel.
     */
    private void addComponents() {
        setLayout(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.LINE_START;
        gbc.weightx = 1f;
        gbc.weighty = 1f;
        gbc.gridwidth = 4;
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.insets = new Insets(5, 2, 5, 3);
        gbc.fill = GridBagConstraints.BOTH;

        final JScrollPane scrollPane = new JScrollPane(m_kafkaTable.getComponent(),
            ScrollPaneConstants.VERTICAL_SCROLLBAR_AS_NEEDED, ScrollPaneConstants.HORIZONTAL_SCROLLBAR_NEVER);

        add(new JPanel().add(scrollPane), gbc);

        ++gbc.gridy;
        gbc.gridwidth = 1;
        gbc.weighty = 0;
        gbc.anchor = GridBagConstraints.CENTER;
        add(m_addButton, gbc);
        ++gbc.gridx;
        add(m_addAllButton, gbc);
        ++gbc.gridx;
        add(m_removeButton, gbc);
        ++gbc.gridx;
        add(m_removeAllButton, gbc);
    }

    /**
     * Toggle the buttons according to the current state of the table.
     */
    private void toggleButtons() {
        setEnabledRemoveButtons(!m_kafkaTable.isEmpty());
        setEnabledAddButtons(m_kafkaTable.hasMoreRows());
    }

    /**
     * Invokes actions if the add button is clicked.
     */
    private void onAdd() {
        try {
            m_kafkaTable.stopEditing();
        } catch (InvalidSettingsException ex) {
            LOGGER.debug(ex.getMessage());
        }
        onAddAction();
    }

    /**
     * Invokes actions if the add button is clicked. Subclass should override this method if another action is needed.
     */
    private void onAddAction() {
        if (m_kafkaTable.addRow()) {
            toggleButtons();
        }
    }

    /**
     * Invokes actions if the add all button is clicked.
     */
    private void onAddAll() {
        try {
            m_kafkaTable.stopEditing();
        } catch (InvalidSettingsException ex) {
            LOGGER.debug(ex.getMessage());
        }
        onAddAllAction();
    }

    /**
     * Invokes actions if the add all button is clicked. Subclass should override this method if another action is
     * needed.
     */
    private void onAddAllAction() {
        if (m_kafkaTable.addAllRows()) {
            toggleButtons();
        }
    }

    /**
     * Invokes actions if the remove button is clicked.
     */
    private void onRemove() {
        try {
            m_kafkaTable.stopEditing();
        } catch (InvalidSettingsException ex) {
            LOGGER.debug(ex.getMessage());
        }
        onRemoveAction();
    }

    /**
     * Invokes actions if the remove button is clicked. Subclass should override this method if another action is
     * needed.
     */
    protected void onRemoveAction() {
        m_kafkaTable.removeOnSelection();
        toggleButtons();
    }

    /**
     * Invokes actions if the removeAll button is clicked.
     */
    private void onRemoveAll() {
        try {
            m_kafkaTable.stopEditing();
        } catch (InvalidSettingsException ex) {
            LOGGER.debug(ex.getMessage());
        }
        onRemoveAllAction();
    }

    /**
     * Invokes actions if the removeAll button is clicked. Subclass should override this method if another action is
     * needed.
     */
    private void onRemoveAllAction() {
        m_kafkaTable.removeAllRows();
        toggleButtons();
    }

    /**
     * Enables or disables the add and addAll button
     *
     * @param isEnabled <code>true</code> if the add button should be enabled, otherwise <code>false</code>
     */
    private void setEnabledAddButtons(final boolean isEnabled) {
        m_addButton.setEnabled(isEnabled);
        m_addAllButton.setEnabled(isEnabled);
    }

    /**
     * Enables or disables the remove and removeAll button
     *
     * @param isEnabled <code>true</code> if the remove and removeAll button should be enabled, otherwise
     *            <code>false</code>
     */
    private void setEnabledRemoveButtons(final boolean isEnabled) {
        m_removeButton.setEnabled(isEnabled);
        m_removeAllButton.setEnabled(isEnabled);
    }

    /**
     * Actions invoked during load
     */
    public void onLoad() {
        toggleButtons();
    }

    /**
     * Actions invoked during save
     *
     * @throws InvalidSettingsException
     */
    public void onSave() throws InvalidSettingsException {
        m_kafkaTable.stopEditing();
    }
}
