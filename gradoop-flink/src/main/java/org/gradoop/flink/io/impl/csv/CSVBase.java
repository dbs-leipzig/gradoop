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
 * Base class for CSV data source and sink.
 */
abstract class CSVBase {
  /**
   * Gradoop Flink configuration
   */
  private final GradoopFlinkConfig config;
  /**
   * Complete path to the xsd file.
   */
  private final String xsdPath = CSVBase.class.getResource("/data/csv/csv_format.xsd").getFile();
  /**
   * Complete path to the xml file.
   */
  private final String metaXmlPath;
  /**
   * Path to the directory containing the csv files.
   */
  private String csvDir;

  /**
   * Creates a new data source/sink. Paths can be local (file://) or HDFS (hdfs://).
   *
   * @param metaXmlPath xml file
   * @param csvDir csv directory
   * @param config Gradoop Flink configuration
   */
  CSVBase(String metaXmlPath, String csvDir, GradoopFlinkConfig config) {
    if (config == null) {
      throw new IllegalArgumentException("config must not be null");
    }
    if (metaXmlPath == null) {
      throw new IllegalArgumentException("path to xml must not be null");
    }
    if (csvDir == null) {
      throw new IllegalArgumentException("csv directory must not be null");
    }

    this.config = config;
    this.metaXmlPath = metaXmlPath;
    this.csvDir = csvDir;
  }

  public GradoopFlinkConfig getConfig() {
    return config;
  }

  public String getMetaXmlPath() {
    return metaXmlPath;
  }

  public String getXsdPath() {
    return xsdPath;
  }

  public String getCsvDir() {
    return csvDir;
  }
}
