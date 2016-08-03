/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.io.impl.hbase;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.tuple.ValueOf1;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.hbase.inputformats.EdgeTableInputFormat;
import org.gradoop.flink.io.impl.hbase.inputformats.GraphHeadTableInputFormat;
import org.gradoop.flink.io.impl.hbase.inputformats.VertexTableInputFormat;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.common.storage.impl.hbase.HBaseEPGMStore;

/**
 * Creates an EPGM instance from HBase.
 */
public class HBaseDataSource extends HBaseBase implements DataSource {

  /**
   * Creates a new HBase data source.
   *
   * @param epgmStore HBase store
   * @param config    Gradoop Flink configuration
   */
  public HBaseDataSource(HBaseEPGMStore epgmStore,
    GradoopFlinkConfig config) {
    super(epgmStore, config);
  }

  @Override
  public LogicalGraph getLogicalGraph() {
    return getGraphCollection().reduce(new ReduceCombination());
  }

  @SuppressWarnings("unchecked")
  @Override
  public GraphCollection getGraphCollection() {
    GradoopFlinkConfig config = getFlinkConfig();
    HBaseEPGMStore store = getStore();

    // used for type hinting when loading graph data
    TypeInformation<Tuple1<GraphHead>> graphTypeInfo = new TupleTypeInfo(
      Tuple1.class,
      TypeExtractor.createTypeInfo(config.getGraphHeadFactory().getType()));

    // used for type hinting when loading vertex data
    TypeInformation<Tuple1<Vertex>> vertexTypeInfo = new TupleTypeInfo(
      Tuple1.class,
      TypeExtractor.createTypeInfo(config.getVertexFactory().getType()));

    // used for type hinting when loading edge data
    TypeInformation<Tuple1<Edge>> edgeTypeInfo = new TupleTypeInfo(
      Tuple1.class,
      TypeExtractor.createTypeInfo(config.getEdgeFactory().getType()));


    DataSet<Tuple1<GraphHead>> graphHeads = config.getExecutionEnvironment()
      .createInput(
        new GraphHeadTableInputFormat(
          config.getGraphHeadHandler(), store.getGraphHeadName()),
        graphTypeInfo);

    DataSet<Tuple1<Vertex>> vertices = config.getExecutionEnvironment()
      .createInput(new VertexTableInputFormat(
          config.getVertexHandler(), store.getVertexTableName()),
        vertexTypeInfo);

    DataSet<Tuple1<Edge>> edges = config.getExecutionEnvironment().createInput(
      new EdgeTableInputFormat(
        config.getEdgeHandler(), store.getEdgeTableName()),
      edgeTypeInfo);

    return GraphCollection.fromDataSets(
      graphHeads.map(new ValueOf1<GraphHead>()),
      vertices.map(new ValueOf1<Vertex>()),
      edges.map(new ValueOf1<Edge>()),
      config);
  }

  @Override
  public GraphTransactions getGraphTransactions() {
    return getGraphCollection().toTransactions();
  }
}
