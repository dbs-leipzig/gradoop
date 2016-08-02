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

package org.gradoop.flink.io.impl.json;

import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Base class for file based I/O formats.
 */
abstract class JSONBase {
  /**
   * Gradoop Flink configuration
   */
  private final GradoopFlinkConfig config;
  /**
   * File to write graph heads to
   */
  private final String graphHeadPath;
  /**
   * File to write vertices to
   */
  private final String vertexPath;
  /**
   * File to write edges to
   */
  private final String edgePath;

  /**
   * Creates a new data source/sink. Paths can be local (file://) or HDFS
   * (hdfs://).
   *
   * @param vertexPath    vertex data file
   * @param edgePath      edge data file
   * @param graphHeadPath graph data file
   * @param config        Gradoop Flink configuration
   */
  JSONBase(String graphHeadPath, String vertexPath,
    String edgePath, GradoopFlinkConfig config) {
    if (config == null) {
      throw new IllegalArgumentException("config must not be null");
    }
    if (vertexPath == null) {
      throw new IllegalArgumentException("vertex file must not be null");
    }
    if (edgePath == null) {
      throw new IllegalArgumentException("edge file must not be null");
    }
    this.graphHeadPath = graphHeadPath;
    this.vertexPath = vertexPath;
    this.edgePath = edgePath;
    this.config = config;
  }

  public GradoopFlinkConfig getConfig() {
    return config;
  }

  public String getGraphHeadPath() {
    return graphHeadPath;
  }

  public String getVertexPath() {
    return vertexPath;
  }

  public String getEdgePath() {
    return edgePath;
  }
}
