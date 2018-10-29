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
package org.gradoop.flink.io.impl.csv.indexed;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.conf.Configuration;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVBase;
import org.gradoop.flink.io.impl.csv.functions.CSVLineToEdge;
import org.gradoop.flink.io.impl.csv.functions.CSVLineToGraphHead;
import org.gradoop.flink.io.impl.csv.functions.CSVLineToVertex;
import org.gradoop.flink.io.impl.csv.metadata.MetaData;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A graph data source for CSV files indexed by label.
 *
 * The datasource expects files separated by label, e.g. in the following directory structure:
 *
 * csvRoot
 *   |- Person.csv     # contains all vertices with label 'Person'
 *   |- University.csv # contains all vertices with label 'University'
 *   |- knows.csv      # contains all edges with label 'knows'
 *   |- studyAt.csv    # contains all edges with label 'studyAt'
 *   |- metadata.csv   # Meta data for all data contained in the graph
 */
public class IndexedCSVDataSource extends CSVBase implements DataSource {
  /**
   * HDFS Configuration.
   */
  private final Configuration hdfsConfig;

  /**
   * Creates a new data source. The constructor creates a default HDFS configuration.
   *
   * @param csvPath root path of csv files
   * @param config gradoop configuration
   * @throws IOException on failure
   */
  public IndexedCSVDataSource(String csvPath, GradoopFlinkConfig config) throws IOException {
    this(csvPath, config, new Configuration());
  }

  /**
   * Creates a new data source.
   *
   * @param csvPath root path of csv files
   * @param conf gradoop configuration
   * @param hdfsConf HDFS configuration
   * @throws IOException on failure
   */
  public IndexedCSVDataSource(String csvPath, GradoopFlinkConfig conf, Configuration hdfsConf)
    throws IOException {
    super(csvPath, conf);
    Objects.requireNonNull(hdfsConf);
    this.hdfsConfig = hdfsConf;
  }

  @Override
  public LogicalGraph getLogicalGraph() throws IOException {
    return getGraphCollection().reduce(new ReduceCombination());
  }

  @Override
  public GraphCollection getGraphCollection() throws IOException {
    MetaData metaData = MetaData.fromFile(getMetaDataPath(), hdfsConfig);

    ExecutionEnvironment env = getConfig().getExecutionEnvironment();
    GraphHeadFactory graphHeadFactory = getConfig().getGraphHeadFactory();
    VertexFactory vertexFactory = getConfig().getVertexFactory();
    EdgeFactory edgeFactory = getConfig().getEdgeFactory();

    Map<String, DataSet<GraphHead>> graphHeads = metaData.getGraphLabels().stream()
      .map(label -> Tuple2.of(label, env.readTextFile(getGraphHeadCSVPath(label))
        .map(new CSVLineToGraphHead(graphHeadFactory))
        .withBroadcastSet(MetaData.fromFile(getMetaDataPath(), getConfig()), BC_METADATA)
        .filter(graphHead -> graphHead.getLabel().equals(label))))
      .collect(Collectors.toMap(t -> t.f0, t -> t.f1));

    Map<String, DataSet<Vertex>> vertices = metaData.getVertexLabels().stream()
      .map(label -> Tuple2.of(label, env.readTextFile(getVertexCSVPath(label))
        .map(new CSVLineToVertex(vertexFactory))
        .withBroadcastSet(MetaData.fromFile(getMetaDataPath(), getConfig()), BC_METADATA)
        .filter(vertex -> vertex.getLabel().equals(label))))
      .collect(Collectors.toMap(t -> t.f0, t -> t.f1));

    Map<String, DataSet<Edge>> edges = metaData.getEdgeLabels().stream()
      .map(label -> Tuple2.of(label, env.readTextFile(getEdgeCSVPath(label))
        .map(new CSVLineToEdge(edgeFactory))
        .withBroadcastSet(MetaData.fromFile(getMetaDataPath(), getConfig()), BC_METADATA)
        .filter(edge -> edge.getLabel().equals(label))))
      .collect(Collectors.toMap(t -> t.f0, t -> t.f1));

    return getConfig().getGraphCollectionFactory().fromIndexedDataSets(graphHeads, vertices, edges);
  }
}
