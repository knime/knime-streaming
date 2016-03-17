/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
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
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
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
 *   Mar 17, 2016 (wiswedel): created
 */
package org.knime.core.streaming;

import java.awt.BorderLayout;

import javax.swing.JComponent;
import javax.swing.JFormattedTextField;
import javax.swing.JLabel;
import javax.swing.JSpinner;
import javax.swing.SpinnerNumberModel;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.ViewUtils;
import org.knime.core.node.workflow.NodeExecutionJobManagerPanel;

/**
 *
 * @author wiswedel
 */
final class SimpleStreamerNodeExecutionJobManagerPanel extends NodeExecutionJobManagerPanel {

    private final JSpinner m_chunkSizeSpinner;

    /**
     * @param chunkSizeSpinner
     */
    SimpleStreamerNodeExecutionJobManagerPanel() {
        setLayout(new BorderLayout());
        m_chunkSizeSpinner = new JSpinner(new SpinnerNumberModel(
            SimpleStreamerNodeExecutionSettings.CHUNK_SIZE_DEFAULT, 1, Integer.MAX_VALUE, 10));
        JComponent editor = m_chunkSizeSpinner.getEditor();
        JFormattedTextField jftf = ((JSpinner.DefaultEditor) editor).getTextField();
        jftf.setColumns(8);
        JLabel label = new JLabel("Chunk Size  ");
        add(ViewUtils.getInFlowLayout(label, m_chunkSizeSpinner), BorderLayout.NORTH);
        String descriptionText =
                "<html><body><p>Determines the size of a batch that is collected at each node before <br/>"
                + "it is handed off to the downstream node. Choosing larger values will <br/>"
                + "reduce synchronization (and hence yield better runtime),whereas <br/>"
                + "small values will make sure that less data is in transit/memory.<br/>"
                + "For ordinary data (consisting only of strings and numbers) larger<br/>"
                + "values are preferred.</p><body></html>";
        JLabel description = new JLabel(descriptionText);
        add(description, BorderLayout.CENTER);
    }

    /** {@inheritDoc} */
    @Override
    public void saveSettings(final NodeSettingsWO settings) throws InvalidSettingsException {
        SimpleStreamerNodeExecutionSettings c = new SimpleStreamerNodeExecutionSettings();
        c.setChunkSize((int)m_chunkSizeSpinner.getValue());
        c.saveSettings(settings);
    }

    /** {@inheritDoc} */
    @Override
    public void loadSettings(final NodeSettingsRO settings) {
        SimpleStreamerNodeExecutionSettings c = new SimpleStreamerNodeExecutionSettings();
        c.loadSettingsInDialog(settings);
        m_chunkSizeSpinner.setValue(c.getChunkSize());
    }

    /** {@inheritDoc} */
    @Override
    public void updateInputSpecs(final PortObjectSpec[] inSpecs) {

    }

}
