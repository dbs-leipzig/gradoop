/**
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

package org.gradoop.flink.io.impl.accumulo;

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.storage.impl.accumulo.AccumuloEPGMStore;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.accumulo.outputformats.EdgeOutputFormat;
import org.gradoop.flink.io.impl.accumulo.outputformats.GraphHeadOutputFormat;
import org.gradoop.flink.io.impl.accumulo.outputformats.VertexOutputFormat;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

/**
 * Accumulo DataSink
 */
public class AccumuloDataSink extends AccumuloBase<GraphHead, Vertex, Edge> implements DataSink {

  /**
   * Creates a new Accumulo data source/sink.
   *
   * @param store     store implementation
   */
  public AccumuloDataSink(AccumuloEPGMStore<GraphHead, Vertex, Edge> store) {
    super(store, store.getConfig());
  }

  @Override
  public void write(LogicalGraph logicalGraph) {
    write(logicalGraph, false);
  }

  @Override
  public void write(GraphCollection graphCollection) {
    write(graphCollection, false);
  }

  @Override
  public void write(
    LogicalGraph logicalGraph,
    boolean overwrite
  ) {
    write(getAccumuloConfig().getGraphCollectionFactory().fromGraph(logicalGraph), overwrite);
  }

  @Override
  public void write(
    GraphCollection graphCollection,
    boolean overWrite
  ) {
    graphCollection.getGraphHeads().output(
      new GraphHeadOutputFormat(getAccumuloConfig().getAccumuloProperties()));
    graphCollection.getVertices().output(
      new VertexOutputFormat(getAccumuloConfig().getAccumuloProperties()));
    graphCollection.getEdges().output(
      new EdgeOutputFormat(getAccumuloConfig().getAccumuloProperties()));
  }

}
