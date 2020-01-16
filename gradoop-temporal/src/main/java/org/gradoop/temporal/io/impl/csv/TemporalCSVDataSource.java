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
package org.gradoop.temporal.io.impl.csv;

import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.temporal.io.api.TemporalDataSource;
import org.gradoop.temporal.io.impl.csv.functions.CSVLineToTemporalEdge;
import org.gradoop.temporal.io.impl.csv.functions.CSVLineToTemporalGraphHead;
import org.gradoop.temporal.io.impl.csv.functions.CSVLineToTemporalVertex;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.TemporalGraphCollectionFactory;
import org.gradoop.temporal.util.TemporalGradoopConfig;

/**
 * A graph data source for CSV files storing temporal graphs.
 * <p>
 * This source expects files separated by vertices and edges in the following directory structure:
 * <ul>
 *   <li>{@code csvRoot}<ul>
 *     <li>{@code vertices.csv} - Vertex data</li>
 *     <li>{@code edges.csv} - Edge data</li>
 *     <li>{@code graphs.csv} - Graph head data</li>
 *     <li>{@code metadata.csv} - Metadata for all data contained in the graph</li>
 *   </ul></li>
 * </ul>
 */
public class TemporalCSVDataSource extends CSVDataSource implements TemporalDataSource {

  /**
   * Creates a new temporal CSV data source.
   *
   * @param csvPath path to the directory containing the CSV files
   * @param config  Gradoop Flink configuration
   */
  public TemporalCSVDataSource(String csvPath, TemporalGradoopConfig config) {
    super(csvPath, config);
  }

  @Override
  public TemporalGraph getTemporalGraph() {
    TemporalGraphCollection collection = getTemporalGraphCollection();
    return getConfig().getTemporalGraphFactory()
      .fromDataSets(
        collection.getGraphHeads().first(1), collection.getVertices(), collection.getEdges());
  }

  @Override
  public TemporalGraphCollection getTemporalGraphCollection() {
    TemporalGraphCollectionFactory collectionFactory = getConfig().getTemporalGraphCollectionFactory();
    return getCollection(
      new CSVLineToTemporalGraphHead(collectionFactory.getGraphHeadFactory()),
      new CSVLineToTemporalVertex(collectionFactory.getVertexFactory()),
      new CSVLineToTemporalEdge(collectionFactory.getEdgeFactory()),
     collectionFactory);
  }

  @Override
  protected TemporalGradoopConfig getConfig() {
    return (TemporalGradoopConfig) super.getConfig();
  }
}
