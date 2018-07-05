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
package org.knime.kafka.settings;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.kafka.ui.KafkaProperty;

/**
 * Abstract class storing all settings related to Kafka.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 *
 */
public abstract class AbstractSettingsModelKafka {

    /** The collection of the used model. */
    private final Collection<SettingsModel> m_models;

    /** The Kafka settings model. */
    protected SettingsModelKafka m_kafkaModel;

    /** The list of advanced Kafka properties. */
    protected final List<KafkaProperty> m_advancedProps;

    /**
     * Constructor.
     *
     * @param advancedProps the advanced properties
     */
    protected AbstractSettingsModelKafka(final List<KafkaProperty> advancedProps) {
        m_advancedProps = advancedProps;
        m_models = new LinkedList<>();
    }

    /**
     * Returns the basic properties.
     *
     * @return the basic properties
     */
    abstract protected Properties getBasicProperties();

    /**
     * Sets the proper instance of {@link SettingsModelKafka} and adds it to the models list. Has to be called in the
     * constructor.
     */
    abstract protected void setAndAddKafkaModel();

    /**
     * Adds the given settings model to the models list.
     *
     * @param toAdd the model to be added
     */
    protected final void addModel(final SettingsModel... toAdd) {
        Arrays.stream(toAdd).forEach(m_models::add);
    }

    /**
     * Returns the Kafka settings model.
     *
     * @return the kafka settings model
     */
    public final SettingsModelKafka getKafkaSettingsModel() {
        return m_kafkaModel;
    }

    /**
     * Validates the settings.
     *
     * @param settings the settings to be validated
     * @throws InvalidSettingsException - If settings are incomplete/deficient.
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        for (final SettingsModel model : m_models) {
            model.validateSettings(settings);
        }
    }

    /**
     * Loads the settings.
     *
     * @param settings the settings to be loaded
     * @throws InvalidSettingsException - If settings are incomplete/deficient.
     */
    public final void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        for (final SettingsModel model : m_models) {
            model.loadSettingsFrom(settings);
        }
    }

    /**
     * Saves the models to the provided settings
     *
     * @param settings to write to
     */
    public final void saveSettingsTo(final NodeSettingsWO settings) {
        for (final SettingsModel model : m_models) {
            model.saveSettingsTo(settings);
        }
    }

    /**
     * Returns the properties.
     *
     * @return the properties
     */
    public final Properties getProperties() {
        final Properties props = m_kafkaModel.getProperties();
        props.putAll(getBasicProperties());
        return props;
    }

    /**
     * Returns the set of available Kafka properties that are used for this settings model.
     *
     * @return the set of available Kafka properties used by this settings model
     */
    public final Set<String> getUsedProperties() {
        Set<String> usedProps = m_advancedProps.stream()//
            .map(KafkaProperty::getKey)//
            .collect(Collectors.toSet());
        getBasicProperties().keySet().stream()//
            .map(k -> (String)k)//
            .collect(Collectors.toCollection(() -> usedProps));
        return usedProps;
    }

    /**
     * Returns the advanced properties. The advanced properties do not contain any of the basic properties.
     *
     * @return the advanced properties
     */
    protected final List<KafkaProperty> getAdvancedModelProperties() {
        return Helper4KafkaConfig.removeProperties(m_advancedProps, getBasicProperties());
    }

}
