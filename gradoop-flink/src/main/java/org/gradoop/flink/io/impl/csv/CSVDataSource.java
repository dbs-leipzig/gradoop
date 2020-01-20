/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.functions.CSVLineToEdge;
import org.gradoop.flink.io.impl.csv.functions.CSVLineToElement;
import org.gradoop.flink.io.impl.csv.functions.CSVLineToGraphHead;
import org.gradoop.flink.io.impl.csv.functions.CSVLineToVertex;
import org.gradoop.flink.io.impl.csv.metadata.CSVMetaDataSource;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.epgm.BaseGraphCollectionFactory;
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
    return collection.getGraphFactory()
      .fromDataSets(
        collection.getGraphHeads().first(1), collection.getVertices(), collection.getEdges());
  }

  @Override
  public GraphCollection getGraphCollection() {
    GraphCollectionFactory collectionFactory = getConfig().getGraphCollectionFactory();
    return getCollection(
      new CSVLineToGraphHead(collectionFactory.getGraphHeadFactory()),
      new CSVLineToVertex(collectionFactory.getVertexFactory()),
      new CSVLineToEdge(collectionFactory.getEdgeFactory()),
      collectionFactory);
  }

  /**
   * Create a graph collection from CSV lines using {@link CSVLineToElement} mapper functions.
   *
   * @param csvToGraphHead    A function mapping a CSV line to a graph head.
   * @param csvToVertex       A function mapping a CSV line to a vertex.
   * @param csvToEdge         A function mapping a CSV line to an edge.
   * @param collectionFactory A factory used to create the final graph collection.
   * @param <G>  The graph head type.
   * @param <V>  The vertex type.
   * @param <E>  The edge type.
   * @param <LG> The graph type.
   * @param <GC> The graph collection type.
   * @return A graph collection representing the graph data stored as CSV.
   */
  protected <
    G extends GraphHead,
    V extends Vertex,
    E extends Edge,
    LG extends BaseGraph<G, V, E, LG, GC>,
    GC extends BaseGraphCollection<G, V, E, LG, GC>> GC getCollection(
      CSVLineToElement<G> csvToGraphHead,
      CSVLineToElement<V> csvToVertex,
      CSVLineToElement<E> csvToEdge,
      BaseGraphCollectionFactory<G, V, E, LG, GC> collectionFactory) {

    // Read the meta data
    DataSet<Tuple3<String, String, String>> metaData =
      new CSVMetaDataSource().readDistributed(getMetaDataPath(), getConfig());

    // Read the datasets of each graph element
    DataSet<G> graphHeads = getConfig().getExecutionEnvironment()
      .readTextFile(getGraphHeadCSVPath())
      .map(csvToGraphHead).withBroadcastSet(metaData, BC_METADATA);

    DataSet<V> vertices = getConfig().getExecutionEnvironment()
      .readTextFile(getVertexCSVPath())
      .map(csvToVertex).withBroadcastSet(metaData, BC_METADATA);

    DataSet<E> edges = getConfig().getExecutionEnvironment()
      .readTextFile(getEdgeCSVPath())
      .map(csvToEdge).withBroadcastSet(metaData, BC_METADATA);

    // Create the graph
    return collectionFactory.fromDataSets(graphHeads, vertices, edges);
  }
}
