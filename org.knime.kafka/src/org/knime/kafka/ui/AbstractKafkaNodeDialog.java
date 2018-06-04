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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import javax.swing.Box;
import javax.swing.JPanel;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.kafka.settings.AbstractSettingsModelKafka;

/**
 * The node dialog containing all relevant settings for the various Kafka nodes.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 *
 * @param <T> an instance of {@link AbstractSettingsModelKafka}
 */
public abstract class AbstractKafkaNodeDialog<T extends AbstractSettingsModelKafka> extends NodeDialogPane {

    /** The settings tab name. */
    private static final String TAB_SETTINGS = "Settings";

    /** The advanced tab name. */
    private static final String TAB_ADVANCED_SETTINGS = "Advanced settings";

    /** The default length for string dialog components. */
    protected static final int DEFAULT_INPUT_COMP_WIDTH = 20;

    /** The kafka dialog component shown in the advanced tab. */
    private DialogComponentKafka m_kafkaComp;

    /** The list of all contained components. */
    private final List<DialogComponent> m_components;

    /** The Kafka settings mode. */
    final T m_kafkaSettings;

    /**
     * Constructor.
     *
     * @param kafkaSettings an instance of {@link AbstractSettingsModelKafka}
     */
    protected AbstractKafkaNodeDialog(final T kafkaSettings) {
        // initilize the components list
        m_components = new LinkedList<DialogComponent>();
        // set the settings model
        m_kafkaSettings = kafkaSettings;
        // add the general and advanced tabs
        addTab(TAB_SETTINGS, createSettingsTab());
        addTab(TAB_ADVANCED_SETTINGS, createAdvancedTab());
    }

    /**
     * Returns the settings model used for the settings tab.
     *
     * @return the components to be shown in the settings tab
     */
    abstract protected List<DialogComponent> getSettingComponents();

    /**
     * Returns the Kafka settings model.
     *
     * @return the Kafka settings model
     */
    protected T getModel() {
        return m_kafkaSettings;
    }

    /**
     * Adds a component to the given panel and registers it.
     *
     * @param panel the panel to add to
     * @param gbc the grid bag constraints
     * @param toAdd the dialog component to add
     */
    private void addComponentToPanel(final JPanel panel, final GridBagConstraints gbc,
        final DialogComponent toAdd) {
        gbc.gridy++;
        panel.add(toAdd.getComponentPanel(), gbc);
        registerDialogComponent(toAdd);
    }

    /**
     * Allows to register dialog components so that they automatically saved and loaded.
     *
     * @param comp the component to register
     */
    protected final void registerDialogComponent(final DialogComponent... comp) {
        Arrays.stream(comp)//
            .forEach(m_components::add);
    }

    /**
     * Creates the settings tab.
     *
     * @return the settings tab
     */
    private final JPanel createSettingsTab() {
        // init panel and grid bag constraints
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.LINE_START;
        gbc.weightx = 0;
        gbc.weighty = 0;
        gbc.gridwidth = 1;
        gbc.insets = new Insets(0, 5, 0, 0);
        gbc.fill = GridBagConstraints.NONE;
        gbc.gridy = 0;

        // get the settings and add them to the panel
        for (final DialogComponent comp : getSettingComponents()) {
            addComponentToPanel(panel, gbc, comp);
        }
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

    /**
     * Creates the advanced settings tab.
     *
     * @return the advanced settings tab
     */
    private JPanel createAdvancedTab() {
        // initialize the panel and grid bag cosntraints
        final JPanel panel = new JPanel(new GridBagLayout());

        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.LINE_START;

        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.weightx = 1;
        gbc.weighty = 1;
        gbc.fill = GridBagConstraints.BOTH;

        // create and add the kafka dialog component
        m_kafkaComp = new DialogComponentKafka(m_kafkaSettings.getKafkaSettingsModel());
        addComponentToPanel(panel, gbc, m_kafkaComp);

        // return the panel
        return panel;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected final void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        for (final DialogComponent comp : m_components) {
            comp.saveSettingsTo(settings);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        for (final DialogComponent comp : m_components) {
            comp.loadSettingsFrom(settings, specs);
        }
    }
}
