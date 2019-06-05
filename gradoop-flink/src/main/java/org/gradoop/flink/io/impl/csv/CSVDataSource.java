/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.io.impl.csv;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.functions.CSVLineToEdge;
import org.gradoop.flink.io.impl.csv.functions.CSVLineToGraphHead;
import org.gradoop.flink.io.impl.csv.functions.CSVLineToVertex;
import org.gradoop.flink.io.impl.csv.metadata.CSVMetaDataSource;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.GraphCollectionFactory;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * A graph data source for CSV files.
 * <p>
 * The datasource expects files separated by vertices and edges, e.g. in the following directory
 * structure:
 * <p>
 * csvRoot
 * |- vertices.csv # all vertex data
 * |- edges.csv    # all edge data
 * |- graphs.csv   # all graph head data
 * |- metadata.csv # Meta data for all data contained in the graph
 */
public class CSVDataSource extends CSVBase implements DataSource {

  /**
   * Creates a new CSV data source.
   *
   * @param csvPath path to the directory containing the CSV files
   * @param config  Gradoop Flink configuration
   */
  public CSVDataSource(String csvPath, GradoopFlinkConfig config) {
    super(csvPath, config);
  }

  /**
   * Will use a single graph head of the collection as final graph head for the graph.
   * Issue #1217 (https://github.com/dbs-leipzig/gradoop/issues/1217) will optimize further.
   *
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph getLogicalGraph() {
    GraphCollection collection = getGraphCollection();
    return getConfig().getLogicalGraphFactory()
      .fromDataSets(
        collection.getGraphHeads().first(1), collection.getVertices(), collection.getEdges());
  }

  @Override
  public GraphCollection getGraphCollection() {
    GraphCollectionFactory factory = getConfig().getGraphCollectionFactory();
    DataSet<Tuple3<String, String, String>> metaData =
      new CSVMetaDataSource().readDistributed(getMetaDataPath(), getConfig());

    DataSet<GraphHead> graphHeads = getConfig().getExecutionEnvironment()
      .readTextFile(getGraphHeadCSVPath())
      .map(new CSVLineToGraphHead(factory.getGraphHeadFactory()))
      .withBroadcastSet(metaData, BC_METADATA);

    DataSet<Vertex> vertices = getConfig().getExecutionEnvironment()
      .readTextFile(getVertexCSVPath())
      .map(new CSVLineToVertex(factory.getVertexFactory()))
      .withBroadcastSet(metaData, BC_METADATA);

    DataSet<Edge> edges = getConfig().getExecutionEnvironment()
      .readTextFile(getEdgeCSVPath())
      .map(new CSVLineToEdge(factory.getEdgeFactory()))
      .withBroadcastSet(metaData, BC_METADATA);


    return factory.fromDataSets(graphHeads, vertices, edges);
  }
}
