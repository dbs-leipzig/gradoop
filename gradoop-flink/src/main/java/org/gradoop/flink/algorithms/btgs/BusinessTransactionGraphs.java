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
package org.gradoop.flink.algorithms.btgs;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.btgs.functions.BtgMessenger;
import org.gradoop.flink.algorithms.btgs.functions.BtgUpdater;
import org.gradoop.flink.algorithms.btgs.functions.CollectGradoopIds;
import org.gradoop.flink.algorithms.btgs.functions.ComponentToNewBtgId;
import org.gradoop.flink.algorithms.btgs.functions.MasterData;
import org.gradoop.flink.algorithms.btgs.functions.NewBtgGraphHead;
import org.gradoop.flink.algorithms.btgs.functions.SetBtgId;
import org.gradoop.flink.algorithms.btgs.functions.SetBtgIds;
import org.gradoop.flink.algorithms.btgs.functions.TargetIdBtgId;
import org.gradoop.flink.algorithms.btgs.functions.TransactionalData;
import org.gradoop.flink.algorithms.gelly.functions.VertexToGellyVertexWithGradoopId;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.flink.model.impl.functions.epgm.ExpandGradoopIds;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.ToGellyEdgeWithNullValue;
import org.gradoop.flink.model.impl.functions.tuple.SwitchPair;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;

/**
 * Part of the BIIIG approach.
 * EPGMVertex-centric implementation to isolate business transaction graphs.
 */
public class BusinessTransactionGraphs implements
  UnaryGraphToCollectionOperator {

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
  public GraphCollection execute(LogicalGraph iig) {

    DataSet<Vertex> masterVertices = iig.getVertices()
      .filter(new MasterData<>());

    LogicalGraph transGraph = iig
      .vertexInducedSubgraph(new TransactionalData<>());

    DataSet<Vertex> transVertices = transGraph
      .getVertices();

    DataSet<org.apache.flink.graph.Edge<GradoopId, NullValue>> transEdges =
      transGraph.getEdges().map(new ToGellyEdgeWithNullValue());

    Graph<GradoopId, GradoopId, NullValue> gellyTransGraph = Graph.fromDataSet(
      transVertices.map(new VertexToGellyVertexWithGradoopId()),
      transEdges,
      iig.getConfig().getExecutionEnvironment()
    );

    gellyTransGraph = gellyTransGraph
      .getUndirected()
      .runScatterGatherIteration(new BtgMessenger(), new BtgUpdater() , 100);


    DataSet<Tuple2<GradoopId, GradoopIdSet>> btgVerticesMap = gellyTransGraph
      .getVerticesAsTuple2()
      .map(new SwitchPair<>())
      .groupBy(0)
      .reduceGroup(new CollectGradoopIds())
      .map(new ComponentToNewBtgId());

    DataSet<Tuple2<GradoopId, GradoopId>> vertexBtgMap = btgVerticesMap
      .flatMap(new ExpandGradoopIds<>())
      .map(new SwitchPair<>());

    DataSet<GraphHead> graphHeads = btgVerticesMap
      .map(new Value0Of2<>())
      .map(new NewBtgGraphHead<>(iig.getConfig().getGraphHeadFactory()));

    // filter and update edges
    DataSet<Edge> btgEdges = iig.getEdges()
      .join(vertexBtgMap)
      .where(new SourceId<>()).equalTo(0)
      .with(new SetBtgId<>());

    // update transactional vertices
    transVertices = transVertices
      .join(vertexBtgMap)
      .where(new Id<>()).equalTo(0)
      .with(new SetBtgId<>());

    // create master data BTG map
    vertexBtgMap = btgEdges
      .map(new TargetIdBtgId<>())
      .join(masterVertices)
      .where(0).equalTo(new Id<>())
      .with(new LeftSide<>())
      .distinct();

    DataSet<Tuple2<GradoopId, GradoopIdSet>> vertexBtgsMap = vertexBtgMap
      .groupBy(0)
      //.combineGroup(new CollectGradoopIds())
      .reduceGroup(new CollectGradoopIds());

    masterVertices = masterVertices.join(vertexBtgsMap)
      .where(new Id<>()).equalTo(0)
      .with(new SetBtgIds<>());

    return iig.getConfig().getGraphCollectionFactory()
      .fromDataSets(graphHeads, transVertices.union(masterVertices), btgEdges);
  }

  @Override
  public String getName() {
    return BusinessTransactionGraphs.class.getName();
  }
}
