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
package org.gradoop.flink.io.impl.edgelist;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.edgelist.functions.CreateImportEdge;
import org.gradoop.flink.io.impl.edgelist.functions.CreateImportVertex;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;

/**
 * Data source to create a {@link LogicalGraph} from an edge list.
 *
 * 0 1
 * 2 0
 *
 * The example line denotes an edge between two vertices:
 * First edge:
 * source: with id = 0
 * target: with id = 1
 *
 * Second edge:
 * source: with id = 2
 * target: with id = 0
 */
public class EdgeListDataSource implements DataSource {
  /**
   * Path to edge list file
   */
  private String edgeListPath;
  /**
   * Separator token of the tsv file
   */
  private String tokenSeparator;
  /**
   * Gradoop Flink configuration
   */
  private GradoopFlinkConfig config;

  /**
   * Creates a new data source. Paths can be local (file://) or HDFS (hdfs://).
   *
   * @param edgeListPath    Path to edge list file
   * @param tokenSeparator  Id separator token
   * @param config          Gradoop Flink configuration
   */
  public EdgeListDataSource(String edgeListPath, String tokenSeparator,
    GradoopFlinkConfig config) {
    this.edgeListPath = edgeListPath;
    this.tokenSeparator = tokenSeparator;
    this.config = config;
  }

  @Override
  public LogicalGraph getLogicalGraph() throws IOException {
    return getGraphCollection().reduce(new ReduceCombination());
  }

  @Override
  public GraphCollection getGraphCollection() throws IOException {

    ExecutionEnvironment env = getConfig().getExecutionEnvironment();

    //--------------------------------------------------------------------------
    // generate tuple that contains all information
    //--------------------------------------------------------------------------

    DataSet<Tuple2<Long, Long>> lineTuples = env
      .readCsvFile(getEdgeListPath())
      .fieldDelimiter(getTokenSeparator())
      .types(Long.class, Long.class);

    //--------------------------------------------------------------------------
    // generate vertices
    //--------------------------------------------------------------------------

    DataSet<ImportVertex<Long>> importVertices = lineTuples
      .<Tuple1<Long>>project(0)
      .union(lineTuples.project(1))
      .distinct()
      .map(new CreateImportVertex<>());

    //--------------------------------------------------------------------------
    // generate edges
    //--------------------------------------------------------------------------

    DataSet<ImportEdge<Long>> importEdges = DataSetUtils
      .zipWithUniqueId(lineTuples)
      .map(new CreateImportEdge<>());

    //--------------------------------------------------------------------------
    // create graph data source
    //--------------------------------------------------------------------------

    return new GraphDataSource<>(importVertices, importEdges, getConfig())
      .getGraphCollection();
  }

  GradoopFlinkConfig getConfig() {
    return config;
  }

  String getEdgeListPath() {
    return edgeListPath;
  }

  String getTokenSeparator() {
    return tokenSeparator;
  }
}
