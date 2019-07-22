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
package org.gradoop.flink.io.impl.deprecated.json;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.deprecated.json.functions.JSONToEdge;
import org.gradoop.flink.io.impl.deprecated.json.functions.JSONToGraphHead;
import org.gradoop.flink.io.impl.deprecated.json.functions.JSONToVertex;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.GraphCollectionFactory;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
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
    GraphCollectionFactory factory = getConfig().getGraphCollectionFactory();

    // used for type hinting when loading vertex data
    TypeInformation<EPGMVertex> vertexTypeInfo = TypeExtractor
      .createTypeInfo(factory.getVertexFactory().getType());
    // used for type hinting when loading edge data
    TypeInformation<EPGMEdge> edgeTypeInfo = TypeExtractor
      .createTypeInfo(factory.getEdgeFactory().getType());
    // used for type hinting when loading graph data
    TypeInformation<EPGMGraphHead> graphTypeInfo = TypeExtractor
      .createTypeInfo(factory.getGraphHeadFactory().getType());

    // read vertex, edge and graph data
    DataSet<EPGMVertex> vertices = env.readTextFile(getVertexPath())
      .map(new JSONToVertex(factory.getVertexFactory()))
      .returns(vertexTypeInfo);
    DataSet<EPGMEdge> edges = env.readTextFile(getEdgePath())
      .map(new JSONToEdge(factory.getEdgeFactory()))
      .returns(edgeTypeInfo);
    DataSet<EPGMGraphHead> graphHeads;
    if (getGraphHeadPath() != null) {
      graphHeads = env.readTextFile(getGraphHeadPath())
        .map(new JSONToGraphHead(factory.getGraphHeadFactory()))
        .returns(graphTypeInfo);
    } else {
      graphHeads = env.fromElements(factory.getGraphHeadFactory().createGraphHead());
    }

    return factory.fromDataSets(graphHeads, vertices, edges);
  }
}
