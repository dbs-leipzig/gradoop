/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.io.impl.csv.indexed;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.hadoop.conf.Configuration;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVBase;
import org.gradoop.flink.io.impl.csv.metadata.CSVMetaData;
import org.gradoop.flink.io.impl.csv.metadata.CSVMetaDataSource;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.temporal.io.api.TemporalDataSource;
import org.gradoop.temporal.io.impl.csv.functions.CSVLineToTemporalEdge;
import org.gradoop.temporal.io.impl.csv.functions.CSVLineToTemporalGraphHead;
import org.gradoop.temporal.io.impl.csv.functions.CSVLineToTemporalVertex;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.layout.TemporalIndexedLayout;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A temporal graph data source for CSV files indexed by label.
 * <p>
 * The datasource expects files separated by label, e.g. in the following directory structure:
 * <p>
 * {@code
 * csvRoot
 * |- vertices
 *   |- person
 *     |- data.csv    # contains all vertices with label 'Person'
 *   |- university
 *     |- data.csv    # contains all vertices with label 'University'
 * |- edges
 *   |- knows
 *     |- data.csv    # contains all edges with label 'knows'
 *   |- studyAt
 *     |- data.csv    # contains all edges with label 'studyAt'
 * |- graphs
 *   |- myGraph
 *     |- data.csv
 * |- metadata.csv   # Meta data for all data contained in the graph
 * }
 */
public class TemporalIndexedCSVDataSource extends CSVBase implements TemporalDataSource, DataSource {

  /**
   * HDFS Configuration.
   */
  private Configuration hdfsConfig = new Configuration();

  /**
   * Creates a new data source. The constructor creates a default HDFS configuration.
   *
   * @param csvPath root path of csv files
   * @param config  gradoop configuration
   */
  public TemporalIndexedCSVDataSource(String csvPath, TemporalGradoopConfig config) {
    super(csvPath, config);
  }

  /**
   * Reads a {@link TemporalGraph} and converts it to a {@link LogicalGraph} via function
   * {@link TemporalGraph##getLogicalGraph()}.
   *
   * @return a logical graph instance
   * @throws IOException in case of an error while reading the graph
   */
  @Override
  public LogicalGraph getLogicalGraph() throws IOException {
    return getTemporalGraph().toLogicalGraph();
  }

  /**
   * Reads a {@link TemporalGraphCollection} and converts it to a {@link GraphCollection} via function
   * {@link TemporalGraphCollection##getGraphCollection()}.
   *
   * @return a graph collection instance
   * @throws IOException in case of an error while reading the graph collection
   */
  @Override
  public GraphCollection getGraphCollection() throws IOException {
    return getTemporalGraphCollection().toGraphCollection();
  }

  @Override
  public TemporalGraph getTemporalGraph() throws IOException {
    return getConfig().getTemporalGraphFactory().fromLayout(readIndexedCSV());
  }

  @Override
  public TemporalGraphCollection getTemporalGraphCollection() throws IOException {
    return getConfig().getTemporalGraphCollectionFactory().fromLayout(readIndexedCSV());
  }

  /**
   * Reads the graph data from (distributed) file system.
   *
   * @return a temporal indexed layout containing the graph/collection data
   * @throws IOException in case of an error while reading
   */
  private TemporalIndexedLayout readIndexedCSV() throws IOException {
    CSVMetaDataSource source = new CSVMetaDataSource();
    CSVMetaData metaData = source.readLocal(getMetaDataPath(), hdfsConfig);
    DataSet<Tuple3<String, String, String>> metaDataBroadcast =
      source.readDistributed(getMetaDataPath(), getConfig());

    ExecutionEnvironment env = getConfig().getExecutionEnvironment();

    Map<String, DataSet<TemporalGraphHead>> graphHeads = metaData.getGraphLabels().stream()
      .map(label -> Tuple2.of(label, env.readTextFile(getGraphHeadCSVPath(label))
        .map(new CSVLineToTemporalGraphHead(getConfig().getTemporalGraphFactory().getGraphHeadFactory()))
        .withBroadcastSet(metaDataBroadcast, BC_METADATA)
        .filter(graphHead -> graphHead.getLabel().equals(label))))
      .collect(Collectors.toMap(t -> t.f0, t -> t.f1));

    Map<String, DataSet<TemporalVertex>> vertices = metaData.getVertexLabels().stream()
      .map(label -> Tuple2.of(label, env.readTextFile(getVertexCSVPath(label))
        .map(new CSVLineToTemporalVertex(getConfig().getTemporalGraphFactory().getVertexFactory()))
        .withBroadcastSet(metaDataBroadcast, BC_METADATA)
        .filter(vertex -> vertex.getLabel().equals(label))))
      .collect(Collectors.toMap(t -> t.f0, t -> t.f1));

    Map<String, DataSet<TemporalEdge>> edges = metaData.getEdgeLabels().stream()
      .map(label -> Tuple2.of(label, env.readTextFile(getEdgeCSVPath(label))
        .map(new CSVLineToTemporalEdge(getConfig().getTemporalGraphFactory().getEdgeFactory()))
        .withBroadcastSet(metaDataBroadcast, BC_METADATA)
        .filter(edge -> edge.getLabel().equals(label))))
      .collect(Collectors.toMap(t -> t.f0, t -> t.f1));

    return new TemporalIndexedLayout(graphHeads, vertices, edges);
  }

  /**
   * Sets a hdfs config used for reading metadata.
   *
   * @param hdfsConfig the config file
   */
  public void setHdfsConfig(Configuration hdfsConfig) {
    this.hdfsConfig = hdfsConfig;
  }

  @Override
  protected TemporalGradoopConfig getConfig() {
    return (TemporalGradoopConfig) super.getConfig();
  }
}
