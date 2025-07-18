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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.kafka.settings.AbstractClientIDSettingsModelKafka;
import org.knime.kafka.settings.AbstractSettingsModelKafka;

/**
 * The node dialog containing the Kafka client ID for consumer and producer nodes.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 *
 * @param <T> and instance of {@link AbstractClientIDSettingsModelKafka}
 */
class AbstractKafkaClientIDDialog<T extends AbstractClientIDSettingsModelKafka> extends AbstractKafkaNodeDialog<T> {

    /** The client id component label. */
    private static final String CLIENT_ID_COMP_LABEL = "Client ID";

    /**
     * Constructor.
     *
     * @param kafkaSettings an instance of {@link AbstractSettingsModelKafka}
     */
    protected AbstractKafkaClientIDDialog(final T kafkaSettings) {
        super(kafkaSettings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<Component> getSettingComponents() {
        final DialogComponent[] diaComps = new DialogComponent[]{ //
            new DialogComponentString(getModel().getClientIDSettingsModel(), CLIENT_ID_COMP_LABEL, false,
                DEFAULT_STRING_INPUT_COMP_WIDTH) //
        };

        // register the components
        registerDialogComponent(diaComps);

        // return the list
        return Arrays.stream(diaComps)//
            .map(DialogComponent::getComponentPanel)//
            .collect(Collectors.toCollection(ArrayList<Component>::new));
    }

}
