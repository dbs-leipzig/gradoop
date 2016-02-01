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
import org.gradoop.model.impl.algorithms.btgs.functions.*;
import org.gradoop.model.impl.functions.epgm.ExpandGradoopIds;
import org.gradoop.model.impl.functions.epgm.GellyEdgeWithoutPayload;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.functions.epgm.SourceId;
import org.gradoop.model.impl.functions.join.LeftSide;
import org.gradoop.model.impl.functions.tuple.SwitchPair;
import org.gradoop.model.impl.functions.tuple.Value0Of2;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;

/**
 * Created by peet on 01.02.16.
 */
public class BusinessTransactionGraphs
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryGraphToCollectionOperator<G, V, E> {

  public static final String SUPERTYPE_KEY = "superType";
  public static final String SUPERCLASS_VALUE_TRANSACTIONAL = "T";
  public static final String SUPERCLASS_VALUE_MASTER = "M";
  public static final String BTG_LABEL = "BusinessTransactionGraph";

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
      .map(new GellyEdgeWithoutPayload<E>());

    Graph<GradoopId, GradoopId, NullValue> gellyTransGraph = Graph.fromDataSet(
      transVertices.map(new BtgVertex<V>()),
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
      .map(new BtgId());

    DataSet<Tuple2<GradoopId, GradoopId>> vertexBtgMap = btgVerticesMap
      .flatMap(new ExpandGradoopIds())
      .map(new SwitchPair<GradoopId, GradoopId>());

    DataSet<G> graphHeads = btgVerticesMap
      .map(new Value0Of2<GradoopId, GradoopIdSet>())
      .map(new Btg<>(iig.getConfig().getGraphHeadFactory()));

    // filter and update edges
    DataSet<E> btgEdges = iig.getEdges()
      .join(vertexBtgMap)
      .where(new SourceId<E>()).equalTo(0)
      .with(new SetBtg<E>());

    // update transactional vertices
    transVertices = transVertices
      .join(vertexBtgMap)
      .where(new Id<V>()).equalTo(0)
      .with(new SetBtg<V>());

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
      .with(new SetBtgs<V>());

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
