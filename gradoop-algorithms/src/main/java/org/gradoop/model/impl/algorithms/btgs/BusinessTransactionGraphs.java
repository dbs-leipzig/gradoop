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

package org.gradoop.model.impl.algorithms.btgs;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.algorithms.btgs.functions.NewBtgGraphHead;
import org.gradoop.model.impl.algorithms.btgs.functions.ComponentToNewBtgId;
import org.gradoop.model.impl.algorithms.btgs.functions.BtgMessenger;
import org.gradoop.model.impl.algorithms.btgs.functions.BtgUpdater;
import org.gradoop.model.impl.algorithms.btgs.functions.ToGellyVertexWithIdValue;
import org.gradoop.model.impl.algorithms.btgs.functions.CollectGradoopIds;
import org.gradoop.model.impl.algorithms.btgs.functions.MasterData;
import org.gradoop.model.impl.algorithms.btgs.functions.SetBtgId;
import org.gradoop.model.impl.algorithms.btgs.functions.SetBtgIds;
import org.gradoop.model.impl.algorithms.btgs.functions.TargetIdBtgId;
import org.gradoop.model.impl.algorithms.btgs.functions.TransactionalData;
import org.gradoop.model.impl.functions.epgm.ExpandGradoopIds;
import org.gradoop.model.impl.functions.epgm.ToGellyEdgeWithNullValue;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.functions.epgm.SourceId;
import org.gradoop.model.impl.functions.join.LeftSide;
import org.gradoop.model.impl.functions.tuple.SwitchPair;
import org.gradoop.model.impl.functions.tuple.Value0Of2;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;

/**
 * Part of the BIIIG approach.
 * Vertex-centric implementation to isolate business transaction graphs.
 *
 * @param <G> graph type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class BusinessTransactionGraphs
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryGraphToCollectionOperator<G, V, E> {

  /**
   * reserved property key referring to master or transactional data
   */
  public static final String SUPERTYPE_KEY = "superType";
  /**
   * reserved property value to mark master data
   */
  public static final String SUPERCLASS_VALUE_MASTER = "M";
  /**
   * reserved property value to mark transactional data
   */
  public static final String SUPERCLASS_VALUE_TRANSACTIONAL = "T";
  /**
   * reserved label to mark business transaction graphs
   */
  public static final String BTG_LABEL = "BusinessTransactionGraph";
  /**
   * reserved property key referring to the source identifier of vertices
   */
  public static final String SOURCEID_KEY = "sid";

  @Override
  public GraphCollection execute(LogicalGraph<G, V, E> iig) {

    DataSet<V> masterVertices = iig.getVertices()
      .filter(new MasterData<V>());

    LogicalGraph<G, V, E> transGraph = iig
      .vertexInducedSubgraph(new TransactionalData<V>());

    DataSet<V> transVertices = transGraph
      .getVertices();

    DataSet<Edge<GradoopId, NullValue>> transEdges = transGraph
      .getEdges()
      .map(new ToGellyEdgeWithNullValue<E>());

    Graph<GradoopId, GradoopId, NullValue> gellyTransGraph = Graph.fromDataSet(
      transVertices.map(new ToGellyVertexWithIdValue<V>()),
      transEdges,
      iig.getConfig().getExecutionEnvironment()
    );

    gellyTransGraph = gellyTransGraph
      .getUndirected()
      .runVertexCentricIteration(new BtgUpdater(), new BtgMessenger(), 100);


    DataSet<Tuple2<GradoopId, GradoopIdSet>> btgVerticesMap = gellyTransGraph
      .getVerticesAsTuple2()
      .map(new SwitchPair<GradoopId, GradoopId>())
      .groupBy(0)
      .reduceGroup(new CollectGradoopIds())
      .map(new ComponentToNewBtgId());

    DataSet<Tuple2<GradoopId, GradoopId>> vertexBtgMap = btgVerticesMap
      .flatMap(new ExpandGradoopIds())
      .map(new SwitchPair<GradoopId, GradoopId>());

    DataSet<G> graphHeads = btgVerticesMap
      .map(new Value0Of2<GradoopId, GradoopIdSet>())
      .map(new NewBtgGraphHead<>(iig.getConfig().getGraphHeadFactory()));

    // filter and update edges
    DataSet<E> btgEdges = iig.getEdges()
      .join(vertexBtgMap)
      .where(new SourceId<E>()).equalTo(0)
      .with(new SetBtgId<E>());

    // update transactional vertices
    transVertices = transVertices
      .join(vertexBtgMap)
      .where(new Id<V>()).equalTo(0)
      .with(new SetBtgId<V>());

    // create master data BTG map
    vertexBtgMap = btgEdges
      .map(new TargetIdBtgId<E>())
      .join(masterVertices)
      .where(0).equalTo(new Id<V>())
      .with(new LeftSide<Tuple2<GradoopId, GradoopId>, V>())
      .distinct();

    DataSet<Tuple2<GradoopId, GradoopIdSet>> vertexBtgsMap = vertexBtgMap
      .groupBy(0)
      //.combineGroup(new CollectGradoopIds())
      .reduceGroup(new CollectGradoopIds());

    masterVertices = masterVertices.join(vertexBtgsMap)
      .where(new Id<V>()).equalTo(0)
      .with(new SetBtgIds<V>());

    return GraphCollection.fromDataSets(
      graphHeads,
      transVertices.union(masterVertices),
      btgEdges,
      iig.getConfig()
    );
  }

  @Override
  public String getName() {
    return null;
  }
}
