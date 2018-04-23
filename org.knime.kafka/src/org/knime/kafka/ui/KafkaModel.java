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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.activation.UnsupportedDataTypeException;
import javax.swing.table.AbstractTableModel;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.knime.core.node.InvalidSettingsException;

/**
 * The Kafka model storing the selected additional properties and their values.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 *
 */
public final class KafkaModel extends AbstractTableModel {

    /** Default uid. */
    private static final long serialVersionUID = 1L;

    /** The add row exception text. */
    private static final String ADD_ROW_EXCEPTION = "The row to be added has the wrong number of entries";

    /** The validate row exception text. */
    private static final String INVALID_ROW_LENGTH_EXCEPTION =
        "The row to be validated has the wrong number of entries";

    /** The column index of the Kafka properties key in the table. */
    public static final int KEY_IDX = 0;

    /** The column index of the Kafka properties value in the table. */
    public static final int VAL_IDX = 1;

    /** The column names */
    private static final String[] COLUMN_NAMES = new String[]{"Key", "Value"};

    /** The column classes */
    private static final Class<?>[] COLUMN_CLASSES = new Class<?>[]{String.class, String.class};

    /** The stored data */
    private List<String[]> m_data;

    /** The property pool holding all available and blocked entries */
    private KafkaRowPool m_propPool;

    /**
     * Constructor.
     *
     * @param properties the available properties
     */
    public KafkaModel(final List<KafkaProperty> properties) {
        super();
        m_data = new ArrayList<String[]>();
        m_propPool = new KafkaRowPool(properties);
    }

    /**
     * Constructor used for cloning.
     */
    private KafkaModel() {
        super();
    }

    /**
     * Creates a clone of this model.
     *
     * @return a deep-copy of this model
     */
    public KafkaModel createClone() {
        final KafkaModel model = new KafkaModel();
        model.m_data = new ArrayList<String[]>();
        for (final String[] row : m_data) {
            model.m_data.add(row.clone());
        }
        model.m_propPool = m_propPool.createClone();
        return model;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final int getColumnCount() {
        return COLUMN_NAMES.length;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final String getColumnName(final int column) {
        return COLUMN_NAMES[column];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Class<?> getColumnClass(final int columnIndex) {
        return COLUMN_CLASSES[columnIndex];
    }

    /**
     * Removes the provided rows from this model.
     *
     * @param rowIndex the indices of the rows to remove
     */
    void removeRows(final int... rowIndex) {
        // sort the indices and remove them from the highest to the lowest
        Arrays.sort(rowIndex);
        for (int i = rowIndex.length - 1; i >= 0; i--) {
            // remove the row from the data and make that row available again
            m_propPool.unlock(m_data.remove(rowIndex[i])[KEY_IDX]);
        }
        // fire update event
        fireTableDataChanged();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isCellEditable(final int rowIndex, final int columnIndex) {
        if (checkRanges(rowIndex, columnIndex)) {
            // only allow editing if the value is also among the allowed rows
            return m_propPool.isValid(getRow(rowIndex)[KEY_IDX]);
        }
        return false;
    }

    /**
     * Returns the tooltip text for the given row and column index.
     *
     * @param rowIndex the row index
     * @param columnIndex the column index
     * @return the tooltip text
     */
    String getToolTipText(final int rowIndex, final int columnIndex) {
        if (checkRanges(rowIndex, columnIndex)) {
            return m_propPool.getToolTipText(getRow(rowIndex)[KEY_IDX]);
        }
        return null;
    }

    /**
     * Adds a row to this model.
     *
     * @return <code>True</code> if a new row has been added to this model and <code>False</code> otherwise
     */
    boolean addRow() {
        // check if there are more rows available
        if (m_propPool.hasMoreRows()) {
            // create new row
            addRow(m_propPool.createRow());
            // fire update event
            fireTableRowsInserted(getRowCount() - 1, getRowCount() - 1);
            return true;
        }
        return false;
    }

    /**
     * Adds all available rows to this model.
     *
     * @return <code>True</code> if new rows have been added to this model and <code>False</code> otherwise
     *
     */
    boolean addAllRows() {
        // check if there are more rows available
        if (m_propPool.hasMoreRows()) {
            // store previous size
            int prevSize = getRowCount();

            // add all available rows
            KafkaProperty property;
            while ((property = m_propPool.createRow()) != null) {
                addRow(property);
            }

            // fire update events
            fireTableRowsInserted(prevSize, getRowCount() - 1);
            return true;
        }
        return false;
    }

    /**
     * Add a row using the provided Kafka property.
     *
     * @param property the property to be added
     */
    private void addRow(final KafkaProperty property) {
        try {
            addRow(new String[]{property.getKey(), property.getValue()});
        } catch (InvalidSettingsException e) {
            // cannot happen
        }
    }

    /**
     * Add a row using the provided String array
     *
     * @param row the row to add
     * @throws InvalidSettingsException - If the row to be added has the wrong number of elements
     */
    public void addRow(final String[] row) throws InvalidSettingsException {
        if (row.length != getColumnCount()) {
            throw new InvalidSettingsException(ADD_ROW_EXCEPTION);
        }
        m_propPool.lock(row[KEY_IDX]);
        m_data.add(row);

    }

    /**
     * Removes all rows from this model.
     */
    void removeAllRows() {
        int rowCount = getRowCount() - 1;
        clear();
        fireTableRowsDeleted(0, rowCount);
    }

    /**
     * Returns <code>true</code> if this model contains more rows.
     *
     * @return <code>true</code> if this model contains more rows
     */
    boolean hasMoreRows() {
        return m_propPool.hasMoreRows();
    }

    /**
     * Returns <code>true</code> if this model contains no elements.
     *
     * @return <code>true</code> if this model contains no elements
     */
    boolean isEmpty() {
        return m_data.isEmpty();
    }

    /**
     * Returns the number of rows stored by this model.
     */
    @Override
    public int getRowCount() {
        return m_data.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setValueAt(final Object aValue, final int rowIndex, final int columnIndex) {
        // check the ranges
        if (checkRanges(rowIndex, columnIndex)) {
            // get the row
            final String[] row = getRow(rowIndex);
            final String newVal = (String)aValue;

            switch (columnIndex) {
                case KEY_IDX:
                    // only update if the value changed
                    final String prevVal = row[KEY_IDX];
                    if (!newVal.equals(prevVal)) {
                        m_propPool.unlock(prevVal);
                        m_propPool.lock(newVal);
                        // a new key also refers to a new value
                        row[KEY_IDX] = newVal;
                        row[VAL_IDX] = m_propPool.getValue(newVal);
                        fireTableCellUpdated(rowIndex, KEY_IDX);
                        fireTableCellUpdated(rowIndex, VAL_IDX);
                    }
                    break;
                case VAL_IDX:
                    if (!newVal.equals(row[VAL_IDX])) {
                        row[VAL_IDX] = aValue.toString();
                        fireTableCellUpdated(rowIndex, VAL_IDX);
                    }
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getValueAt(final int rowIndex, final int columnIndex) {
        if (checkRanges(rowIndex, columnIndex)) {
            return getRow(rowIndex)[columnIndex];
        }
        return null;
    }

    /**
     * Returns the row for the given index
     *
     * @param rowIndex the index of the row to be returned
     * @return the row for the given index
     */
    private String[] getRow(final int rowIndex) {
        return m_data.get(rowIndex);
    }

    /**
     * Checks whether the provided row and column index are valid.
     *
     * @param rowIndex the row index
     * @param columnIndex the column index
     * @return <code>True</code> if the indices lie within the range
     */
    private boolean checkRanges(final int rowIndex, final int columnIndex) {
        return (rowIndex >= 0 && rowIndex < getRowCount() && columnIndex >= 0 && columnIndex < getColumnCount());
    }

    /**
     * Returns the row provider for the given column index.
     *
     * @param columnIndex the column index
     * @return the row provider for the given column index
     */
    RowProvider getProvider(final int columnIndex) {
        if (columnIndex == KEY_IDX) {
            return m_propPool;
        }
        return null;
    }

    /**
     * Returns the properties stored by this model.
     *
     * @return the properties stored by this model
     */
    public Properties getProperties() {
        final Properties prop = new Properties();
        for (final String[] row : m_data) {
            prop.put(row[KEY_IDX], row[VAL_IDX]);
        }
        return prop;

    }

    /**
     * Returns the data stored by this model.
     *
     * @return the data stored by this model
     */
    public List<String[]> getData() {
        return m_data;
    }

    /**
     * Clears this model.
     */
    public void clear() {
        // clear all data
        m_data.clear();
        // make all rows available again
        m_propPool.unlockAll();
    }

    /**
     * Validates the correctness of the value.
     *
     * @param aValue the value to be validated
     * @param rowIndex the row index
     * @param columnIndex the column index
     * @throws Exception - If the value is not valid
     */
    void validate(final Object aValue, final int rowIndex, final int columnIndex) throws Exception {
        if (checkRanges(rowIndex, columnIndex)) {
            final String newVal = (String)aValue;
            switch (columnIndex) {
                case KEY_IDX:
                    // check if the value is among the allowed rows
                    if (!m_propPool.isValid(newVal)) {
                        throw new UnsupportedDataTypeException("The key: " + newVal + " is not supported anymore");
                    }
                    break;
                case VAL_IDX:
                    // check if the value fulfills the ConfigDef requirements
                    final String key = getRow(rowIndex)[KEY_IDX];
                    ConfigDef.parseType(key, newVal, m_propPool.getType(key));
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * Validates the correctness of the data stored by this model.
     *
     * @throws InvalidSettingsException - If any of the rows contains invalid values
     */
    public void validate() throws InvalidSettingsException {
        for (final String[] row : m_data) {
            validate(row);
        }
    }

    /**
     * Validates the correctness of the given row.
     *
     * @param row the row to be validated
     * @throws InvalidSettingsException - If the row contains invalid values
     */
    public void validate(final String[] row) throws InvalidSettingsException {
        // check if the row has the proper number of entries
        if (row.length != getColumnCount()) {
            throw new InvalidSettingsException(INVALID_ROW_LENGTH_EXCEPTION);
        }
        // test if the key and value are valid
        final String key = row[KEY_IDX];
        if (m_propPool.isValid(key)) {
            try {
                ConfigDef.parseType(key, row[VAL_IDX], m_propPool.getType(key));
            } catch (final ConfigException e) {
                throw new InvalidSettingsException(e.getMessage());
            }
        } else {
            throw new InvalidSettingsException(
                "The key: " + key + " is not supported anymore. Please remove the corresponding entry");
        }
    }

    /**
     * Block some of the available properties.
     *
     * @param toBlock the properties to block
     */
    public void blockProperties(final Set<String> toBlock) {
        m_propPool.blockRows(toBlock);
    }
}
