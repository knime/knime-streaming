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

import java.util.Optional;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.util.CheckUtils;

/**
 * Proxy object that represents the job manager's configuration.
 *
 * @author Bernd Wiswedel, KNIME.com, Zurich, Switzerland
 * @noreference This class is not intended to be referenced by clients.
 */
public final class SimpleStreamerNodeExecutionSettings {

    static final int CHUNK_SIZE_DEFAULT = Optional.ofNullable(
        Integer.getInteger("knime.core.streaming.chunksize")).orElse(50).intValue();

    private int m_chunkSize = CHUNK_SIZE_DEFAULT;

    void saveSettings(final NodeSettingsWO settings) {
        settings.addInt("chunk-size", m_chunkSize);
    }

    void loadSettingsInModel(final NodeSettingsRO settings) throws InvalidSettingsException {
        // added in 3.2
        int chunkSize = settings.getInt("chunk-size", CHUNK_SIZE_DEFAULT);
        CheckUtils.checkSetting(chunkSize > 0, "Chunk size must be larger than 0: %d", chunkSize);
        m_chunkSize = chunkSize;
    }


    void loadSettingsInDialog(final NodeSettingsRO settings) {
        int chunkSize = settings.getInt("chunk-size", CHUNK_SIZE_DEFAULT);
        if (chunkSize <= 0) {
            chunkSize = CHUNK_SIZE_DEFAULT;
        }
        m_chunkSize = chunkSize;
    }

    /** @param chunkSize the chunkSize to set */
    void setChunkSize(final int chunkSize) throws InvalidSettingsException {
        CheckUtils.checkSetting(chunkSize > 0, "Chunk size must be larger 0: %d", chunkSize);
        m_chunkSize = chunkSize;
    }

    /** @return the chunkSize */
    public int getChunkSize() {
        return m_chunkSize;
    }

}
