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

import javax.swing.DefaultCellEditor;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JTable;
import javax.swing.ListCellRenderer;

/**
 * Cell editor showing all available Kafka property entries.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 *
 */
final class KafkaCellEditor extends DefaultCellEditor {

	/** Default uid. */
	private static final long serialVersionUID = 1L;

	/** The entry provider */
	final RowProvider m_provider;

	/**
	 * Constructor.
	 *
	 * @param provider
	 *            the available entry provider
	 */
	KafkaCellEditor(final RowProvider provider) {
		super(new JComboBox<String>());
		m_provider = provider;
		getComboBox().setRenderer(new KafkaListCellRenderer());
	}

	/**
	 * Returns the proper combobox class.
	 *
	 * @return the combobox
	 */
	@SuppressWarnings("unchecked")
	private JComboBox<String> getComboBox() {
		return (JComboBox<String>) getComponent();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Component getTableCellEditorComponent(final JTable table, final Object value, final boolean isSelected, final int row, final int column) {
		// get the combobox
		final JComboBox<String> box = getComboBox();

		// clear all items
		box.removeAllItems();

		// add all available items plus the currently shown value
		for (final String key : m_provider.getSelectableRows((String) value)) {
			box.addItem(key);
		}

		return super.getTableCellEditorComponent(table, value, isSelected, row, column);
	}

	/**
	 * List cell renderer showing tooltips for each of the selected entries.
	 *
	 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
	 *
	 */
	private class KafkaListCellRenderer extends JLabel implements ListCellRenderer<String> {

		/** Default uid. */
		private static final long serialVersionUID = 1L;

		/**
		 * Constructor.
		 */
		private KafkaListCellRenderer() {
			setOpaque(true);
			setVerticalAlignment(CENTER);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public Component getListCellRendererComponent(final JList<? extends String> list, final String value, final int index,
				final boolean isSelected, final boolean cellHasFocus) {
			// adapt background
			if (isSelected) {
				setBackground(list.getSelectionBackground());
				setForeground(list.getSelectionForeground());
			} else {
				setBackground(list.getBackground());
				setForeground(list.getForeground());
			}
			// set the new value
			setText(value);
			// set the tooltip text
			setToolTipText(m_provider.getToolTipText(value));
			return this;
		}
	}

}
