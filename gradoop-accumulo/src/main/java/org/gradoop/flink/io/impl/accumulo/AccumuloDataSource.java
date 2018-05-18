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

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.storage.impl.accumulo.AccumuloEPGMStore;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.accumulo.inputformats.EdgeInputFormat;
import org.gradoop.flink.io.impl.accumulo.inputformats.GraphHeadInputFormat;
import org.gradoop.flink.io.impl.accumulo.inputformats.VertexInputFormat;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.GraphCollectionFactory;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;

/**
 * Accumulo DataSource
 */
public class AccumuloDataSource extends AccumuloBase<GraphHead, Vertex, Edge> implements
  DataSource {

  /**
   * Creates a new HBase data source/sink.
   *  @param store store implementation
   */
  public AccumuloDataSource(AccumuloEPGMStore<GraphHead, Vertex, Edge> store) {
    super(store, store.getConfig());
  }

  @Override
  public LogicalGraph getLogicalGraph() {
    return getGraphCollection().reduce(new ReduceCombination());
  }

  @Override
  public GraphCollection getGraphCollection() {
    GraphCollectionFactory factory = getAccumuloConfig().getGraphCollectionFactory();
    ExecutionEnvironment env = getAccumuloConfig().getExecutionEnvironment();
    return factory.fromDataSets(
      /*graph head format*/
      env.createInput(new GraphHeadInputFormat(getStore().getConfig().getAccumuloProperties())),
      /*vertex input format*/
      env.createInput(new VertexInputFormat(getStore().getConfig().getAccumuloProperties())),
      /*edge input format*/
      env.createInput(new EdgeInputFormat(getStore().getConfig().getAccumuloProperties())));
  }

}
