
package org.gradoop.flink.io.impl.csv;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
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
  CSVBase(String csvPath, GradoopFlinkConfig config) {
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

  /**
   * Reads the meta data from the specified file.
   *
   * @param path path to meta data csv file
   * @return meta data information
   */
  DataSet<Tuple2<String, String>> readMetaData(String path) {
    return getConfig().getExecutionEnvironment()
      .readTextFile(path)
      .map(line -> {
          String[] tokens = line.split(CSVConstants.TOKEN_DELIMITER, 2);
          return Tuple2.of(tokens[0], tokens[1]);
        })
      .returns(new TypeHint<Tuple2<String, String>>() { });
  }
}
