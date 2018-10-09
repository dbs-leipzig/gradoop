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
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.conf.Configuration;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
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
 *   |- studyAy.csv    # contains all edges with label 'studyAt'
 *   |- metadata.csv   # Meta data for all data contained in the graph
 */
@Deprecated
public class LogicalGraphIndexedCSVDataSource extends LogicalGraphCSVBase implements DataSource {
  /**
   * HDFS Configuration
   */
  private final Configuration hdfsConfig;

  /**
   * Creates a new data source. The constructor creates a default HDFS configuration.
   *
   * @param csvPath root path of csv files
   * @param config gradoop configuration
   */
  public LogicalGraphIndexedCSVDataSource(String csvPath, GradoopFlinkConfig config) {
    this(csvPath, config, new Configuration());
  }

  /**
   * Creates a new data source.
   *
   * @param csvPath root path of csv files
   * @param conf gradoop configuration
   * @param hdfsConf HDFS configuration
   */
  public LogicalGraphIndexedCSVDataSource(
    String csvPath, GradoopFlinkConfig conf, Configuration hdfsConf) {
    super(csvPath, conf);
    Objects.requireNonNull(hdfsConf);
    this.hdfsConfig = hdfsConf;
  }

  @Override
  public LogicalGraph getLogicalGraph() throws IOException {
    MetaData metaData = MetaData.fromFile(getMetaDataPath(), hdfsConfig);

    ExecutionEnvironment env = getConfig().getExecutionEnvironment();
    VertexFactory vertexFactory = getConfig().getVertexFactory();
    EdgeFactory edgeFactory = getConfig().getEdgeFactory();

    Map<String, DataSet<Vertex>> vertices = metaData.getVertexLabels().stream()
      .map(l -> Tuple2.of(l, env.readTextFile(getVertexCSVPath(l))
        .map(new CSVLineToVertex(vertexFactory))
        .withBroadcastSet(MetaData.fromFile(getMetaDataPath(), getConfig()), BC_METADATA)))
      .collect(Collectors.toMap(t -> t.f0, t -> t.f1));

    Map<String, DataSet<Edge>> edges = metaData.getEdgeLabels().stream()
      .map(l -> Tuple2.of(l, env.readTextFile(getEdgeCSVPath(l))
        .map(new CSVLineToEdge(edgeFactory))
        .withBroadcastSet(MetaData.fromFile(getMetaDataPath(), getConfig()), BC_METADATA)))
      .collect(Collectors.toMap(t -> t.f0, t -> t.f1));

    return getConfig().getLogicalGraphFactory().fromIndexedDataSets(vertices, edges);
  }

  @Override
  public GraphCollection getGraphCollection() throws IOException {
    return getConfig().getGraphCollectionFactory().fromGraph(getLogicalGraph());
  }
}
