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

import java.io.File;
import java.util.Objects;

/**
 * Base class for CSV data source and data sink.
 */
public abstract class CSVBase {
  /**
   * Broadcast set identifier for meta data.
   */
  public static final String BC_METADATA = "metadata";
  /**
   * CSV file for vertices.
   */
  private static final String VERTEX_FILE = "vertices.csv";
  /**
   * CSV file for edges.
   */
  private static final String EDGE_FILE = "edges.csv";
  /**
   * CSV file for meta data.
   */
  private static final String METADATA_FILE = "metadata.csv";
  /**
   * Path to the vertex CSV file.
   */
  private final String vertexCSVPath;
  /**
   * Path to the edge CSV file.
   */
  private final String edgeCSVPath;
  /**
   * Path to the meta data CSV file.
   */
  private final String metaDataPath;
  /**
   * Gradoop Flink configuration
   */
  private final GradoopFlinkConfig config;

  /**
   * Constructor.
   *
   * @param csvPath directory to the CSV files
   * @param config Gradoop Flink configuration
   */
  public CSVBase(String csvPath, GradoopFlinkConfig config) {
    Objects.requireNonNull(csvPath);
    Objects.requireNonNull(config);
    this.vertexCSVPath = csvPath + File.separator + VERTEX_FILE;
    this.edgeCSVPath = csvPath + File.separator + EDGE_FILE;
    this.metaDataPath = csvPath + File.separator + METADATA_FILE;
    this.config = config;
  }

  String getVertexCSVPath() {
    return vertexCSVPath;
  }

  String getEdgeCSVPath() {
    return edgeCSVPath;
  }

  String getMetaDataPath() {
    return metaDataPath;
  }

  GradoopFlinkConfig getConfig() {
    return config;
  }
}
