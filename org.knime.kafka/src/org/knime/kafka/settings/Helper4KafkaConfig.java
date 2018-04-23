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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.knime.kafka.ui.KafkaProperty;

/**
 * Helper class to create and manipulate {@link KafkaProperty KafkaProperties}.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 *
 */
final class Helper4KafkaConfig {

    /** The config field name. */
    private static final String CONFIG_FIELD_NAME = "CONFIG";

    /** The modifier field name. */
    private static final String MODIFIER_FIELD_NAME = "modifiers";

    /**
     * Returns all the properties provided by the given class and transforms the to {@link KafkaProperty
     * KafkaProperties}. Same as {@link #getProperties(Class, Set) getProperties(cl, Collections.<String>emptySet())}.
     *
     *
     * @param cl the class containing the properties
     * @return the properties provided by the given class
     */
    static List<KafkaProperty> getProperties(final Class<? extends AbstractConfig> cl) {
        return getProperties(cl, Collections.<String> emptySet());
    }

    /**
     * Returns all the properties provided by the given class, excluding any blacklisted properties, and converts them
     * to {@link KafkaProperty KafkaProperties}. Since by default Kafka does not allow to access these properties this
     * requires reflection. If reflection fails a fall-back routine is executed creating properties without default
     * values.
     *
     * @param cl the class containing the properties
     * @param blackList set of values to be removed from the list
     * @return the properties provided by the given class
     */
    static List<KafkaProperty> getProperties(final Class<? extends AbstractConfig> cl, final Set<String> blackList) {
        ConfigDef cfg = null;
        try {
            final Field f = cl.getDeclaredField(CONFIG_FIELD_NAME);
            f.setAccessible(true);
            // remove final modifiers
            Field modifiersField = Field.class.getDeclaredField(MODIFIER_FIELD_NAME);
            modifiersField.setAccessible(true);
            modifiersField.setInt(f, f.getModifiers() & ~Modifier.FINAL);
            // get the config def
            cfg = (ConfigDef)f.get(null);
        } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
            // nothing to do here
        }

        final List<KafkaProperty> kafkaProperties;

        // if the reflection worked out map the properties to Kafka properties
        if (cfg != null) {
            kafkaProperties = cfg.configKeys().values().stream()//
                .filter(cfgKey -> !blackList.contains(cfgKey.displayName))//
                .map(cfgKey -> new KafkaProperty(cfgKey.displayName, cfgKey.defaultValue, cfgKey.type,
                    cfgKey.documentation, cfgKey.hasDefault()))//
                .collect(Collectors.toList());
        } else {
            // get the list of config names and transform it to Kafka properties
            final Set<String> cfgNames;
            if (cl == ProducerConfig.class) {
                cfgNames = ProducerConfig.configNames();
            } else if (cl == ConsumerConfig.class) {
                cfgNames = ConsumerConfig.configNames();
            } else {
                cfgNames = Collections.<String> emptySet();
            }
            kafkaProperties = cfgNames.stream()//
                .map(name -> new KafkaProperty(name, "", ConfigDef.Type.STRING, null, false))//
                .collect(Collectors.toList());
        }

        // return the properties
        return kafkaProperties;

    }

    /**
     * Returns the set of common {@link KafkaProperty KafkaProperties} for the provided classes. Same as
     * {@link #getCommonProperties(Class, Class, Set) getCommonProperties(cl1,cl2, Collections.<String>emptySet()}.
     *
     * @param cl1 the one class containing the properties
     * @param cl2 the other class containing the properties
     * @return the common properties of the provided classes
     */
    static List<KafkaProperty> getCommonProperties(final Class<? extends AbstractConfig> cl1,
        final Class<? extends AbstractConfig> cl2) {
        return getCommonProperties(cl1, cl2, Collections.<String> emptySet());
    }

    /**
     * Returns the set of common {@link KafkaProperty KafkaProperties} for the provided classes excluding any black
     * listed property.
     *
     * @param cl1 the one class containing the properties
     * @param cl2 the other class containing the properties
     * @param blackList set of values to be removed from the list
     * @return the common properties of the provided classes
     */
    static List<KafkaProperty> getCommonProperties(final Class<? extends AbstractConfig> cl1,
        final Class<? extends AbstractConfig> cl2, final Set<String> blackList) {
        // get the Kafka properties for the second class
        final List<KafkaProperty> cl2props = getProperties(cl2, blackList);
        // keep only those properties of cl1 that are also properties of cl2
        return getProperties(cl1, blackList).stream()//
            .filter(p -> cl2props.contains(p))//
            .collect(Collectors.toList());
    }

    /**
     * Removes a set of {@link Properties} from a list of {@link KafkaProperty KafkaProperties}.
     *
     * @param properties the list containing to remove from
     * @param toRemove the entires to remove
     * @return the list of {@link KafkaProperty KafkaProperties} after the removal
     */
    static List<KafkaProperty> removeProperties(final List<KafkaProperty> properties, final Properties toRemove) {
        // remove all entries from properties that are keys in toRemove
        return properties.stream()//
            .filter(p -> !toRemove.containsKey(p.getKey()))//
            .collect(Collectors.toList());
    }

}
