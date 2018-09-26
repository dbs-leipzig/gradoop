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
package org.gradoop.flink.io.impl.deprecated.json;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.deprecated.json.functions.JSONToEdge;
import org.gradoop.flink.io.impl.deprecated.json.functions.JSONToGraphHead;
import org.gradoop.flink.io.impl.deprecated.json.functions.JSONToVertex;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Creates an EPGM instance from JSON files. The exact format is documented in
 * {@link JSONToGraphHead}, {@link JSONToVertex}, {@link JSONToEdge}.
 *
 * @deprecated This class is deprecated. For example use
 * {@link org.gradoop.flink.io.impl.csv.CSVDataSource}
 */
@Deprecated
public class JSONDataSource extends JSONBase implements DataSource {

  /**
   * Creates a new data source. The graph is written from the specified path which points to a
   * directory containing the JSON files. Paths can be local (file://) or HDFS (hdfs://).
   *
   * @param inputPath directory containing JSON files
   * @param config    Gradoop Flink configuration
   */
  public JSONDataSource(String inputPath, GradoopFlinkConfig config) {
    this(inputPath + DEFAULT_GRAPHS_FILE,
      inputPath + DEFAULT_VERTEX_FILE,
      inputPath + DEFAULT_EDGE_FILE,
      config);
  }

  /**
   * Creates a new data source. Paths can be local (file://) or HDFS (hdfs://).
   *
   * @param graphHeadPath graph data file
   * @param vertexPath    vertex data file
   * @param edgePath      edge data file
   * @param config        Gradoop Flink configuration
   */
  public JSONDataSource(String graphHeadPath, String vertexPath,
    String edgePath, GradoopFlinkConfig config) {
    super(graphHeadPath, vertexPath, edgePath, config);
  }

  @Override
  public LogicalGraph getLogicalGraph() {
    return getGraphCollection().reduce(new ReduceCombination());
  }

  @Override
  public GraphCollection getGraphCollection() {
    ExecutionEnvironment env = getConfig().getExecutionEnvironment();

    // used for type hinting when loading vertex data
    TypeInformation<Vertex> vertexTypeInfo = TypeExtractor
      .createTypeInfo(getConfig().getVertexFactory().getType());
    // used for type hinting when loading edge data
    TypeInformation<Edge> edgeTypeInfo = TypeExtractor
      .createTypeInfo(getConfig().getEdgeFactory().getType());
    // used for type hinting when loading graph data
    TypeInformation<GraphHead> graphTypeInfo = TypeExtractor
      .createTypeInfo(getConfig().getGraphHeadFactory().getType());

    // read vertex, edge and graph data
    DataSet<Vertex> vertices = env.readTextFile(getVertexPath())
      .map(new JSONToVertex(getConfig().getVertexFactory()))
      .returns(vertexTypeInfo);
    DataSet<Edge> edges = env.readTextFile(getEdgePath())
      .map(new JSONToEdge(getConfig().getEdgeFactory()))
      .returns(edgeTypeInfo);
    DataSet<GraphHead> graphHeads;
    if (getGraphHeadPath() != null) {
      graphHeads = env.readTextFile(getGraphHeadPath())
        .map(new JSONToGraphHead(getConfig().getGraphHeadFactory()))
        .returns(graphTypeInfo);
    } else {
      graphHeads = env.fromElements(
        getConfig().getGraphHeadFactory().createGraphHead());
    }

    return getConfig().getGraphCollectionFactory()
      .fromDataSets(graphHeads, vertices, edges);
  }
}
