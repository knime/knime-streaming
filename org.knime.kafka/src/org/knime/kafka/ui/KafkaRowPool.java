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

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.knime.core.util.DuplicateKeyException;

/**
 * Class managing the list of available {@link KafkaProperty KafkaProperties}
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 *
 */
final class KafkaRowPool implements RowProvider {

    /** The key already locked exception text. */
    private static final String LOCKED_EXCEPTION = "The key is already locked";

    /** The list of available Kafka properties. */
    private final List<KafkaProperty> m_allProperties;

    /** Set storing the keys of the used Kafka properties. */
    private HashSet<String> m_used;

    /** The list of selectable Kafka properties. */
    private LinkedHashMap<String, KafkaProperty> m_selectableProps;

    /**
     * Constructor.
     *
     * @param properties the list of potential properties
     */
    KafkaRowPool(final List<KafkaProperty> properties) {
        m_allProperties = properties;
        m_used = new HashSet<String>();
        m_selectableProps = new LinkedHashMap<String, KafkaProperty>();
        Collections.sort(m_allProperties);
        m_allProperties.stream()//
            .forEachOrdered(k -> m_selectableProps.put(k.getKey(), k));
    }

    /**
     * Constructor used for cloning.
     */
    private KafkaRowPool() {
        m_allProperties = new LinkedList<KafkaProperty>();
    }

    /**
     * Creates a deep copy of this pool.
     *
     * @return a deep copy of this pool
     */
    KafkaRowPool createClone() {
        // init
        final KafkaRowPool pool = new KafkaRowPool();
        // copy the used keys
        pool.m_used = new HashSet<String>(m_used);

        // copy all properties
        for (final KafkaProperty prop : m_allProperties) {
            pool.m_allProperties.add(prop.createClone());
        }
        // copy the selectable properties
        pool.m_selectableProps = new LinkedHashMap<String, KafkaProperty>();
        for (Entry<String, KafkaProperty> entry : m_selectableProps.entrySet()) {
            pool.m_selectableProps.put(entry.getKey(), entry.getValue().createClone());
        }
        return pool;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getSelectableRows(final String curEntry) {
        return m_selectableProps.keySet().stream()//
            .filter(p -> !m_used.contains(p) || p.equals(curEntry))//
            .collect(Collectors.toList());
    }

    /**
     * Blocks the provided row keys from the set of selectable rows.
     *
     * @param toBlock the keys to block
     */
    void blockRows(final Set<String> toBlock) {
        // release lock on blocked entries
        m_used.removeAll(toBlock);
        // update the available entries
        m_selectableProps.clear();
        m_allProperties.stream()//
            .filter(k -> !toBlock.contains(k.getKey()))//
            .forEach(k -> m_selectableProps.put(k.getKey(), k));
    }

    /**
     * Marks the provided key selectable.
     *
     * @param key the key to be unlocked
     */
    void unlock(final String key) {
        m_used.remove(key);
    }

    /**
     * Marks all entries selectable.
     */
    void unlockAll() {
        m_used.clear();
    }

    /**
     * Marks the provided key unselectable.
     *
     * @param key the key to be locked
     * @return <code>True</code> if the key was successfully locked
     * @throws DuplicateKeyException - If the key is already locked
     */
    boolean lock(final String key) throws DuplicateKeyException {
        if (m_selectableProps.containsKey(key)) {
            if (!m_used.add(key)) {
                throw new DuplicateKeyException(LOCKED_EXCEPTION);
            }
        }
        return false;
    }

    /**
     * Returns <code>true</code> if this pool contains more rows.
     *
     * @return <code>true</code> if this pool contains more rows
     */
    boolean hasMoreRows() {
        return m_selectableProps.size() != m_used.size();
    }

    /**
     * Returns the next selectable row.
     *
     * @return the next selectable row
     */
    KafkaProperty createRow() {
        // find the next unselected row and return it
        for (final KafkaProperty prop : m_selectableProps.values()) {
            if (!m_used.contains(prop.getKey())) {
                return prop;
            }
        }
        return null;
    }

    /**
     * Returns the property value for the given key.
     *
     * @param key the property key
     * @return the value for the given key
     */
    String getValue(final String key) {
        KafkaProperty prop;
        if ((prop = getProperty(key)) != null) {
            return prop.getValue();
        }
        return null;
    }

    /**
     * Returns the {@link ConfigDef} type for the given key.
     *
     * @param key the property key
     * @return the {@link ConfigDef} type for the given key
     */
    ConfigDef.Type getType(final String key) {
        KafkaProperty prop;
        if ((prop = getProperty(key)) != null) {
            return prop.getType();
        }
        return null;

    }

    /**
     * {@inheritDoc}
     *
     */
    @Override
    public String getToolTipText(final String key) {
        KafkaProperty prop;
        if ((prop = getProperty(key)) != null) {
            return prop.getToolTip();
        }
        return null;
    }

    /**
     * Returns the {@link KafkaProperty} for the given key.
     *
     * @param key the property key
     * @return the {@link KafkaProperty} for the given key
     */
    private KafkaProperty getProperty(final String key) {
        return m_selectableProps.get(key);
    }

    /**
     * Returns <code>True</code> if the provided key is a selectable property.
     *
     * @param key the lookup key
     * @return <code>True</code> if the provided key is a selectable property
     */
    boolean hasProperty(final String key) {
        return getProperty(key) != null;
    }

    /**
     * Returns <code>True</code> if the given key specifies one of the selectable properties.
     *
     * @param key the key to test
     * @return <code>True</code> if the given key specifies one of the selectable properties
     */
    boolean isValid(final String key) {
        return m_selectableProps.containsKey(key);
    }

}
