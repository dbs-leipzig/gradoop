/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.flink.io.impl.deprecated.logicalgraphcsv;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * A graph data source for CSV files.
 *
 * The datasource expects files separated by vertices and edges, e.g. in the following directory
 * structure:
 *
 * csvRoot
 *   |- vertices.csv # all vertex data
 *   |- edges.csv    # all edge data
 *   |- metadata.csv # Meta data for all data contained in the graph
 */
@Deprecated
public class LogicalGraphCSVDataSource extends LogicalGraphCSVBase implements DataSource {

  /**
   * Creates a new CSV data source.
   *
   * @param csvPath path to the directory containing the CSV files
   * @param config Gradoop Flink configuration
   */
  public LogicalGraphCSVDataSource(String csvPath, GradoopFlinkConfig config) {
    super(csvPath, config);
  }

  @Override
  public LogicalGraph getLogicalGraph() {
    DataSet<Tuple3<String, String, String>> metaData =
      MetaData.fromFile(getMetaDataPath(), getConfig());

    DataSet<Vertex> vertices = getConfig().getExecutionEnvironment()
      .readTextFile(getVertexCSVPath())
      .map(new CSVLineToVertex(getConfig().getVertexFactory()))
      .withBroadcastSet(metaData, BC_METADATA);

    DataSet<Edge> edges = getConfig().getExecutionEnvironment()
      .readTextFile(getEdgeCSVPath())
      .map(new CSVLineToEdge(getConfig().getEdgeFactory()))
      .withBroadcastSet(metaData, BC_METADATA);

    return getConfig().getLogicalGraphFactory().fromDataSets(vertices, edges);
  }

  @Override
  public GraphCollection getGraphCollection() {
    return getConfig().getGraphCollectionFactory().fromGraph(getLogicalGraph());
  }
}

