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
package org.gradoop.storage.impl.accumulo.io;

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.impl.accumulo.AccumuloEPGMStore;
import org.gradoop.storage.impl.accumulo.io.outputformats.ElementOutputFormat;

import javax.annotation.Nonnull;

/**
 * Write graph or graph collection into accumulo store
 */
public class AccumuloDataSink extends AccumuloBase implements DataSink {

  /**
   * Creates a new Accumulo data sink.
   *
   * @param store     store implementation
   * @param flinkConfig gradoop flink configuration
   */
  public AccumuloDataSink(
    @Nonnull AccumuloEPGMStore store,
    @Nonnull GradoopFlinkConfig flinkConfig
  ) {
    super(store, flinkConfig);
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
  public void write(LogicalGraph logicalGraph, boolean overwrite) {
    write(getFlinkConfig().getGraphCollectionFactory().fromGraph(logicalGraph), overwrite);
  }

  @Override
  public void write(GraphCollection graphCollection, boolean overWrite) {
    graphCollection.getGraphHeads()
      .output(new ElementOutputFormat<>(GraphHead.class, getAccumuloConfig()));
    graphCollection.getVertices()
      .output(new ElementOutputFormat<>(Vertex.class, getAccumuloConfig()));
    graphCollection.getEdges()
      .output(new ElementOutputFormat<>(Edge.class, getAccumuloConfig()));
  }

}
