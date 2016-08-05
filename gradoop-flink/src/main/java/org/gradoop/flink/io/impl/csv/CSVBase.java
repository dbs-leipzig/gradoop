/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.io.impl.csv;


import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Base class for TLF data source and sink.
 *
 */
abstract class CSVBase {
  /**
   * Gradoop Flink configuration
   */
  private final GradoopFlinkConfig config;
  /**
   * File to read/write TLF content to
   */
  private final String metaXmlPath;

  CSVBase(GradoopFlinkConfig config, String metaXmlPath) {
    if (config == null) {
      throw new IllegalArgumentException("config must not be null");
    }
    if (metaXmlPath == null) {
      throw new IllegalArgumentException("csv file must not be null");
    }

    this.config = config;
    this.metaXmlPath = metaXmlPath;
  }

  public GradoopFlinkConfig getConfig() {
    return config;
  }

  public String getMetaXmlPath() {
    return metaXmlPath;
  }
}
