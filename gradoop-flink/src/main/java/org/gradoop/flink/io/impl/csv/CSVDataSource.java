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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.functions.CSVToEdge;
import org.gradoop.flink.io.impl.csv.functions.CSVToVertex;
import org.gradoop.flink.io.impl.csv.metadata.MetaDataParser;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

/**
 * A graph data source for CSV files.
 */
public class CSVDataSource implements DataSource {
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
   * Path where the CSV files are stored
   *
   * @param csvPath path to the directory containing the CSV files
   * @param config Gradoop Flink configuration
   */
  public CSVDataSource(String csvPath, GradoopFlinkConfig config) {
    Objects.requireNonNull(csvPath);
    Objects.requireNonNull(config);
    this.vertexCSVPath = csvPath + File.separator + VERTEX_FILE;
    this.edgeCSVPath = csvPath + File.separator + EDGE_FILE;
    this.metaDataPath = csvPath + File.separator + METADATA_FILE;
    this.config = config;
  }

  @Override
  public LogicalGraph getLogicalGraph() throws IOException {
    DataSet<Tuple2<String, String>> metaData = config.getExecutionEnvironment()
      .readCsvFile(metaDataPath)
      .fieldDelimiter(MetaDataParser.TOKEN_DELIMITER)
      .types(String.class, String.class);

    DataSet<Vertex> vertices = config.getExecutionEnvironment()
      .readCsvFile(vertexCSVPath)
      .fieldDelimiter(CSVToVertex.TOKEN_SEPARATOR)
      .types(String.class, String.class, String.class)
      .map(new CSVToVertex(config.getVertexFactory()))
      .withBroadcastSet(metaData, BC_METADATA);

    DataSet<Edge> edges = config.getExecutionEnvironment()
      .readCsvFile(edgeCSVPath)
      .fieldDelimiter(CSVToEdge.TOKEN_SEPARATOR)
      .types(String.class, String.class, String.class, String.class, String.class)
      .map(new CSVToEdge(config.getEdgeFactory()))
      .withBroadcastSet(metaData, BC_METADATA);

    return LogicalGraph.fromDataSets(vertices, edges, config);
  }

  @Override
  public GraphCollection getGraphCollection() throws IOException {
    return GraphCollection.fromGraph(getLogicalGraph());
  }

  @Override
  public GraphTransactions getGraphTransactions() throws IOException {
    return getGraphCollection().toTransactions();
  }
}
