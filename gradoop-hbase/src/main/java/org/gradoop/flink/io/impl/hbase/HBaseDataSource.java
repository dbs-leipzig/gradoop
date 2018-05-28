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
package org.gradoop.flink.io.impl.hbase;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.config.GradoopHBaseConfig;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.storage.impl.hbase.HBaseEPGMStore;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.hbase.inputformats.EdgeTableInputFormat;
import org.gradoop.flink.io.impl.hbase.inputformats.GraphHeadTableInputFormat;
import org.gradoop.flink.io.impl.hbase.inputformats.VertexTableInputFormat;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.tuple.ValueOf1;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Creates an EPGM instance from HBase.
 */
public class HBaseDataSource extends HBaseBase<GraphHead, Vertex, Edge>
  implements DataSource {

  /**
   * Creates a new HBase data source.
   *
   * @param epgmStore HBase store
   * @param config    Gradoop Flink configuration
   */
  public HBaseDataSource(HBaseEPGMStore<GraphHead, Vertex, Edge> epgmStore,
          GradoopFlinkConfig config) {
    super(epgmStore, config);
  }

  @Override
  public LogicalGraph getLogicalGraph() {
    return getGraphCollection().reduce(new ReduceCombination());
  }

  @Override
  public GraphCollection getGraphCollection() {
    GradoopHBaseConfig config = getHBaseConfig();
    HBaseEPGMStore store = getStore();

    // used for type hinting when loading graph data
    TypeInformation<Tuple1<GraphHead>> graphTypeInfo = new TupleTypeInfo<>(
      TypeExtractor.createTypeInfo(config.getGraphHeadFactory().getType()));

    // used for type hinting when loading vertex data
    TypeInformation<Tuple1<Vertex>> vertexTypeInfo = new TupleTypeInfo<>(
      TypeExtractor.createTypeInfo(config.getVertexFactory().getType()));

    // used for type hinting when loading edge data
    TypeInformation<Tuple1<Edge>> edgeTypeInfo = new TupleTypeInfo<>(
      TypeExtractor.createTypeInfo(config.getEdgeFactory().getType()));


    DataSet<Tuple1<GraphHead>> graphHeads = config.getExecutionEnvironment()
      .createInput(new GraphHeadTableInputFormat<>(config.getGraphHeadHandler(),
        store.getGraphHeadName()), graphTypeInfo);

    DataSet<Tuple1<Vertex>> vertices = config.getExecutionEnvironment()
      .createInput(new VertexTableInputFormat<>(config.getVertexHandler(),
          store.getVertexTableName()), vertexTypeInfo);

    DataSet<Tuple1<Edge>> edges = config.getExecutionEnvironment().createInput(
      new EdgeTableInputFormat<>(config.getEdgeHandler(),
        store.getEdgeTableName()), edgeTypeInfo);

    return config.getGraphCollectionFactory().fromDataSets(
      graphHeads.map(new ValueOf1<>()),
      vertices.map(new ValueOf1<>()),
      edges.map(new ValueOf1<>()));
  }
}
