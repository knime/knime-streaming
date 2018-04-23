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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.text.WordUtils;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Class wrapping the central information of a Kafka property.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 *
 */
public final class KafkaProperty implements Comparable<KafkaProperty> {

	/** The Kafka property key. */
	private final String m_key;

	/** The {@link ConfigDef} type. */
	private final ConfigDef.Type m_type;

	/** The Kafka property value. */
	private final String m_value;

	/** The Kafka property tooltip. */
	private final String m_toolTip;

	/**
	 * Constructor.
	 *
	 * @param key
	 *            the Kafka property key
	 * @param value
	 *            the Kafka property value
	 * @param type
	 *            the {@link ConfigDef} type
	 * @param toolTip
	 *            the Kafka property tooltip
	 * @param hasDefault
	 *            <code>True</code> if a default value is provided
	 */
	public KafkaProperty(final String key, final Object value, final ConfigDef.Type type, final String toolTip,
			final boolean hasDefault) {
		// set the key and the type
		m_key = key;
		m_type = type;

		// set the value to empty string if no default is provided
		if (value == null || !hasDefault) {
			m_value = "";
		} else if (value instanceof String) {
			// if its a string just set it
			m_value = (String) value;
		} else if (m_type == ConfigDef.Type.CLASS) {
			// if it is a class remove the leading class string
			String tmp = value.getClass().getName();
			tmp = tmp.replaceAll("^class", "").trim();
			m_value = tmp;
		} else if (m_type == ConfigDef.Type.LIST) {
			// we know this is a list, hence this cast is save
			if (value instanceof List<?>) {
				final List<?> tmp = (List<?>) value;
				m_value = tmp.stream()//
						.map(v -> convert(v))//
						.collect(Collectors.joining(",", "", ""));
			} else {
				m_value = "";
			}
		} else {
			// otherwise get the string value
			m_value = String.valueOf(value);
		}
		// wrap the text so it can be used as a tooltip
		m_toolTip = ("<html>" + WordUtils.wrap(toolTip, 60) + "</html>").replace("\n", "<br>");
	}

	/**
	 * Constructor.
	 *
	 * @param key
	 *            the Kafka property key
	 * @param value
	 *            the Kafka property value
	 * @param type
	 *            the {@link ConfigDef} type
	 * @param toolTip
	 *            the Kafka property tooltip
	 */
	private KafkaProperty(final String key, final String value, final ConfigDef.Type type, final String toolTip) {
		m_key = key;
		m_value = value;
		m_type = type;
		m_toolTip = toolTip;
	}

	/**
	 * Create a clone of this property
	 *
	 * @return a clone of this property
	 */
	KafkaProperty createClone() {
		return new KafkaProperty(m_key, m_value, m_type, m_toolTip);
	}

	/**
	 * Converts the given object to a string depending on its class.
	 *
	 * @param toConvert
	 *            the object to be converted
	 * @return the converted object
	 */
	private String convert(final Object toConvert) {
		if (toConvert instanceof String) {
			return (String) toConvert;
		} else {
			return String.valueOf(toConvert).replaceAll("^class", "").trim();
		}
	}

	/**
	 * Return the key of this property.
	 *
	 * @return the key of this property
	 */
	public String getKey() {
		return m_key;
	}

	/**
	 * Return the value of this property.
	 *
	 * @return the value of this property
	 */
	String getValue() {
		return m_value;
	}

	/**
	 * Return the {@link ConfigDef} type of this property.
	 *
	 * @return the {@link ConfigDef} type of this property
	 */
	ConfigDef.Type getType() {
		return m_type;
	}

	/**
	 * Return the tooltip of this property.
	 *
	 * @return the tooltip of this property
	 */
	String getToolTip() {
		return m_toolTip;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int compareTo(final KafkaProperty o) {
		return m_key.compareTo(o.m_key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return m_key;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object anObject) {
		if (anObject == this) {
			return true;
		}
		if (anObject instanceof KafkaProperty) {
			KafkaProperty anotherProp = (KafkaProperty) anObject;
			return (m_key.equals(anotherProp.m_key) && m_value.equals(anotherProp.m_value)
					&& m_type.equals(anotherProp.m_type) && m_toolTip.equals(anotherProp.m_toolTip));
		}
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
	    return super.hashCode();
	}

}
