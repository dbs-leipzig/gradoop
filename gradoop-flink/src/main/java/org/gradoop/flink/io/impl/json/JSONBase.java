
package org.gradoop.flink.io.impl.json;

import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Base class for file based I/O formats.
 */
abstract class JSONBase {
  /**
   * Default file name for storing graph heads
   */
  static final String DEFAULT_GRAPHS_FILE = "/graphs.json";
  /**
   * Default file name for storing vertices
   */
  static final String DEFAULT_VERTEX_FILE = "/vertices.json";
  /**
   * Default file name for storing edges
   */
  static final String DEFAULT_EDGE_FILE = "/edges.json";
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
