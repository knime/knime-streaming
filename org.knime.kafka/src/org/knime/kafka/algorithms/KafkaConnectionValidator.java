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
 *   Apr 20, 2018 (Mark Ortmann, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.kafka.algorithms;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.knime.core.node.InvalidSettingsException;

/**
 * Class to test if the provided connection details allow to connect to a Kafka Server/Cluster.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 */
public final class KafkaConnectionValidator {

    /** The validation exception. */
    private static final String VALIDATION_EXCEPTION =
        "Cannot connect to server. Please ensure that the server list and advanced settings were correctly set.";

    /** The connection exception. */
    private static final String CONNECTION_EXCEPTION = "Cannot connect to the server.";

    /** Constructor. */
    private KafkaConnectionValidator() {
        // ensure nobody instantiates this class
    }

    /**
     * Test if the entered properties allow to connect to the server.
     *
     * @param connectionProps the connection properties
     * @param requestTimeout the request timeout used to test the connection
     * @throws InvalidSettingsException - If the connection is not valid
     */
    public static void validateConnection(final Properties connectionProps, final int requestTimeout)
        throws InvalidSettingsException {
        checkConnection(connectionProps, requestTimeout, VALIDATION_EXCEPTION);
    }

    /**
     * Test if the server is still available.
     *
     * @param connectionProps the connection properties
     * @param requestTimeout the request timeout used to test the connection
     * @throws InvalidSettingsException - If the connection is not valid
     */
    public static void testConnection(final Properties connectionProps, final int requestTimeout)
        throws InvalidSettingsException {
        checkConnection(connectionProps, requestTimeout, CONNECTION_EXCEPTION);
    }

    /**
     * Checks the server connection.
     *
     * @param connectionProps the connection properties
     * @param requestTimeout the request timeout used to test the connection
     * @throws InvalidSettingsException - If the connection is not valid
     */
    private static void checkConnection(final Properties connectionProps, final int requestTimeout,
        final String exceptionMsg) throws InvalidSettingsException {
        Properties props = new Properties();
        props.putAll(connectionProps);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, requestTimeout - 1);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, requestTimeout - 1);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 300);
        props.put("key.deserializer", LongDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        try (KafkaConsumer<Long, String> simpleConsumer = new KafkaConsumer<>(props)) {
            simpleConsumer.listTopics();
        } catch (final TimeoutException e) {
            throw new InvalidSettingsException(exceptionMsg, e);
        } catch (final KafkaException e) {
            throw new InvalidSettingsException(e.getMessage(), e);
        }
    }

}
