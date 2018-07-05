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
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.JButton;
import javax.swing.JPanel;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentButtonGroup;
import org.knime.core.node.defaultnodesettings.DialogComponentNumberEdit;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.kafka.algorithms.KNIMEKafkaConsumer;
import org.knime.kafka.algorithms.KNIMEKafkaConsumer.BreakCondition;
import org.knime.kafka.settings.SettingsModelKafkaConsumer;
import org.knime.time.util.DialogComponentDateTimeSelection;
import org.knime.time.util.DialogComponentDateTimeSelection.DisplayOption;

/**
 * The node dialog containing the Kafka client most of the components for the consumer nodes.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 *
 */
public final class KafkaConsumerNodeDialog extends AbstractKafkaClientIDDialog<SettingsModelKafkaConsumer> {

    /** The group id component label. */
    private static final String GROUP_ID_COMP_LABEL = "Group ID";

    /** The poll timeout component label. */
    private static final String POLL_TIMEOUT_COMP_LABEL = "Poll timeout (ms)";

    /** The topics component label. */
    private static final String TOPICS_COMP_LABEL = "Topic(s)";

    /** The topic is pattern component label. */
    private static final String TOPIC_IS_PATTERN_COMP_LABEL = "Topic is pattern";

    /** The max requested record per poll component label. */
    private static final String MAX_NUM_REC_COMP_LABEL = "Max number of messages per poll";

    /** The total number of polled records component label. */
    private static final String TOTAL_NUM_REC_COMP_LABEL = "Total number of messages";

    /** The append message info columns component label. */
    private static final String MSG_INFO_COLUMN_COMP_LABEL = "Append message info columns";

    /** The convert to JSON component label. */
    private static final String CONVERT_TO_JSON_COMP_LABEL = "Convert message to JSON";

    /** The stop execution border title. */
    private static final String POLL_OPTIONS_BORDER_TITLE = "Stop Criterion";

    /** The topics tooltip text. */
    private static final String TOPICS_TOOLTIP =
        "<html>Either a comma-separated list of <tt>topics</tt> or a <tt>regular expression</tt></html>";

    /** The topic options border title. */
    private static final String TOPIC_OPTIONS_BORDER_TITLE = "Topic Options";

    private JButton m_nowBtn;

    /**
     * Constructor.
     *
     * @param kafkaSettings an instance of {@link SettingsModelKafkaConsumer}
     */
    public KafkaConsumerNodeDialog(final SettingsModelKafkaConsumer kafkaSettings) {
        super(kafkaSettings);
        initSettingsTab();
        addListeners();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<Component> getSettingComponents() {
        final JPanel panel = createTopicsPanel();

        final DialogComponent[] diaComps = new DialogComponent[]{ //
            new DialogComponentString(getModel().getGroupIDSettingsModel(), GROUP_ID_COMP_LABEL, false,
                DEFAULT_STRING_INPUT_COMP_WIDTH)//
            , new DialogComponentBoolean(getModel().getConvertToJSONSettingsModel(), CONVERT_TO_JSON_COMP_LABEL)//
            , new DialogComponentNumberEdit(getModel().getMaxReqMsgsSettingsModel(), MAX_NUM_REC_COMP_LABEL,
                DEFAULT_NUMBER_INPUT_COMP_WIDTH)//
            , new DialogComponentNumberEdit(getModel().getPollTimeoutSettingsModel(), POLL_TIMEOUT_COMP_LABEL,
                DEFAULT_NUMBER_INPUT_COMP_WIDTH)//
        };

        // register the components
        registerDialogComponent(diaComps);

        // append the additional dialog components
        final List<Component> comps = super.getSettingComponents();
        final List<Component> extComps = Arrays.stream(diaComps)//
            .map(DialogComponent::getComponentPanel)//
            .collect(Collectors.toCollection(() -> comps));
        // add the panel after the group id and return the list
        extComps.add(2, panel);

        // add the break condition panel at the end
        extComps.add(createBreakConditionPanel());

        // return the components list
        return extComps;
    }

    /**
     * @return
     */
    private Component createBreakConditionPanel() {
        // create the panel and add a border
        final JPanel panel = new JPanel(new GridBagLayout());
        panel
            .setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), POLL_OPTIONS_BORDER_TITLE));

        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.LINE_START;
        gbc.weightx = 0;
        gbc.weighty = 0;
        gbc.gridwidth = 1;
        gbc.insets = new Insets(5, 0, 0, 0);
        gbc.fill = GridBagConstraints.NONE;
        gbc.gridy = 0;
        gbc.gridx = 0;

        // create and register the button group
        final DialogComponentButtonGroup btnGrpDiaComp =
            new DialogComponentButtonGroup(getModel().getBreakConditionSettingsModel(), true, "", Arrays//
                .stream(KNIMEKafkaConsumer.BreakCondition.values())//
                .map(opt -> opt.toString())//
                .toArray(String[]::new));
        registerDialogComponent(btnGrpDiaComp);

        // add msg count button
        panel.add(btnGrpDiaComp.getButton(KNIMEKafkaConsumer.BreakCondition.MSG_COUNT.toString()), gbc);
        ++gbc.gridy;
        ++gbc.gridx;
        gbc.insets = new Insets(0, 10, 0, 0);

        // create/register/add the total number of messages dialog component
        DialogComponentNumberEdit totMsgsDiaComp = new DialogComponentNumberEdit(getModel().getTotalMsgSettingsModel(),
            TOTAL_NUM_REC_COMP_LABEL, DEFAULT_NUMBER_INPUT_COMP_WIDTH);
        registerDialogComponent(totMsgsDiaComp);
        panel.add(totMsgsDiaComp.getComponentPanel(), gbc);
        gbc.insets = new Insets(0, 0, 0, 0);
        ++gbc.gridy;
        gbc.gridx = 0;

        // add the time button
        panel.add(btnGrpDiaComp.getButton(KNIMEKafkaConsumer.BreakCondition.TIME.toString()), gbc);
        ++gbc.gridy;
        ++gbc.gridx;
        gbc.insets = new Insets(0, 10, 0, 0);

        // add the time panel
        panel.add(createTimePanel(), gbc);
        gbc.gridheight = 1;
        ++gbc.gridy;
        gbc.weighty = 1;

        // add dummy component
        panel.add(Box.createVerticalGlue(), gbc);

        // return the panel
        return panel;
    }

    /**
     * Creates the time panel.
     *
     * @return the time panel
     */
    private Component createTimePanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.LINE_START;
        gbc.weightx = 0;
        gbc.weighty = 0;
        gbc.gridwidth = 2;
        gbc.insets = new Insets(0, 0, 0, 0);
        gbc.fill = GridBagConstraints.NONE;
        gbc.gridy = 0;
        gbc.gridx = 0;

        // create/register/add the date time dialog component
        final DialogComponentDateTimeSelection dateTimeDiaComp = new DialogComponentDateTimeSelection(
            getModel().getTimeSettingsModel(), null, DisplayOption.SHOW_DATE_AND_TIME_AND_TIMEZONE);
        registerDialogComponent(dateTimeDiaComp);
        panel.add(dateTimeDiaComp.getComponentPanel(), gbc);
        gbc.gridwidth = 1;
        gbc.gridy += 1;
        gbc.insets = new Insets(0, 0, 0, 0);

        // create/register/add the current time dialog component
        final DialogComponentBoolean curTimeDiaComp =
            new DialogComponentBoolean(getModel().getUseCurTimeSettingsModel(), "Use execution date & time");
        registerDialogComponent(curTimeDiaComp);
        panel.add(curTimeDiaComp.getComponentPanel(), gbc);
        gbc.anchor = GridBagConstraints.LINE_END;
        gbc.insets = new Insets(0, 0, 0, 10);
        ++gbc.gridx;

        // create and add the current time button
        m_nowBtn = new JButton("Current time");
        panel.add(m_nowBtn, gbc);

        return panel;
    }

    /**
     * Creates the topics panel.
     *
     * @return the topics panel
     */
    private JPanel createTopicsPanel() {
        // create the dialog components
        final DialogComponentString topicsDiaComp = new DialogComponentString(getModel().getTopicsSettingsModel(),
            TOPICS_COMP_LABEL, true, DEFAULT_STRING_INPUT_COMP_WIDTH);
        topicsDiaComp.setToolTipText(TOPICS_TOOLTIP);

        final DialogComponent[] diaComps = new DialogComponent[]{
            new DialogComponentBoolean(getModel().getTopicPatternSettingsModel(), TOPIC_IS_PATTERN_COMP_LABEL)//
            , new DialogComponentBoolean(getModel().getAppendTopicColumnSettingsModel(), MSG_INFO_COLUMN_COMP_LABEL)//
        };

        // register them
        registerDialogComponent(topicsDiaComp);
        registerDialogComponent(diaComps);

        // create the panel
        final JPanel panel = new JPanel(new GridBagLayout());

        // add border
        panel.setBorder(
            BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), TOPIC_OPTIONS_BORDER_TITLE));

        // init gbc
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.LINE_START;
        gbc.weightx = 0;
        gbc.weighty = 0;
        gbc.gridwidth = 1;
        gbc.insets = new Insets(0, 5, 0, 0);
        gbc.fill = GridBagConstraints.NONE;
        gbc.gridy = 0;
        gbc.gridx = 0;

        // fill the panel
        gbc.gridwidth = 2;
        panel.add(topicsDiaComp.getComponentPanel(), gbc);
        ++gbc.gridy;

        gbc.gridwidth = 1;
        for (final DialogComponent comp : diaComps) {
            panel.add(comp.getComponentPanel(), gbc);
            ++gbc.gridx;
        }
        return panel;
    }

    /**
     * Adds the listeners.
     */
    private void addListeners() {
        m_nowBtn.addActionListener(l -> getModel().getTimeSettingsModel().setZonedDateTime(
            getModel().getTimeSettingsModel().useMillis() ? ZonedDateTime.now() : ZonedDateTime.now().withNano(0)));
        getModel().getBreakConditionSettingsModel().addChangeListener(l -> toggleBreakCondition());
        getModel().getUseCurTimeSettingsModel().addChangeListener(l -> toggleTimesPanel());
    }

    /**
     * Switches between the break condition panels.
     */
    private void toggleBreakCondition() {
        boolean enableTime = true;
        if (getModel().getBreakConditionSettingsModel().getStringValue().equals(BreakCondition.MSG_COUNT.toString())) {
            enableTime = false;
        }
        // enable/disable total messages dialog component
        getModel().getTotalMsgSettingsModel().setEnabled(!enableTime);

        // enable/disable all time related dialog components
        getModel().getUseCurTimeSettingsModel().setEnabled(enableTime);
        toggleTimesPanel();
    }

    /**
     * Enables/disables the time selection dialog components.
     */
    private void toggleTimesPanel() {
        final boolean enable = !getModel().getUseCurTimeSettingsModel().getBooleanValue()
            && getModel().getUseCurTimeSettingsModel().isEnabled();
        getModel().getTimeSettingsModel().setEnabled(enable);
        m_nowBtn.setEnabled(enable);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final DataTableSpec[] specs)
        throws NotConfigurableException {
        super.loadSettingsFrom(settings, specs);
        toggleBreakCondition();
    }

}