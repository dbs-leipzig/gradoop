package org.gradoop.model.impl.algorithms.btg;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.algorithms.btg.functions.BTGJoin;
import org.gradoop.model.impl.algorithms.btg.functions.BTGKeySelector;
import org.gradoop.model.impl.algorithms.btg.functions.EdgeToBTGEdgeMapper;
import org.gradoop.model.impl.algorithms.btg.functions
  .LongListFromPropertyFunction;
import org.gradoop.model.impl.algorithms.btg.functions.VertexToBTGVertexMapper;
import org.gradoop.model.impl.algorithms.btg.pojos.BTGVertexValue;
import org.gradoop.model.impl.functions.keyselectors.VertexKeySelector;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.auxiliary.OverlapSplitBy;

/**
 * BTG Graph to Collection Operator.
 *
 * Encapsulates {@link BTGAlgorithm} in a Gradoop Operator.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 * @see BTGAlgorithm
 */
public class BTG<VD extends EPGMVertex, ED extends EPGMEdge, GD extends EPGMGraphHead> implements
  UnaryGraphToCollectionOperator<VD, ED, GD> {
  /**
   * BTG ID PropertyKey
   */
  public static final String VERTEX_BTGIDS_PROPERTYKEY = "vertex.btgid";
  /**
   * BTGType PropertyKey
   */
  public static final String VERTEX_TYPE_PROPERTYKEY = "vertex.btgtype";
  /**
   * BTGValue PropertyKey
   */
  public static final String VERTEX_VALUE_PROPERTYKEY = "vertex.btgvalue";
  /**
   * Counter to define maximal Iteration for the Algorithm
   */
  private int maxIterations;

  /**
   * Constructor
   *
   * @param maxIterations int defining maximal step counter
   */
  public BTG(int maxIterations) {
    this.maxIterations = maxIterations;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<GD, VD, ED> execute(
    final LogicalGraph<GD, VD, ED> graph) throws Exception {
    DataSet<Vertex<GradoopId, BTGVertexValue>> vertices =
      graph.getVertices().map(new VertexToBTGVertexMapper<VD>());
    DataSet<Edge<GradoopId, NullValue>> edges =
      graph.getEdges().map(new EdgeToBTGEdgeMapper<ED>());
    Graph<GradoopId, BTGVertexValue, NullValue> btgGraph =
      Graph.fromDataSet(vertices, edges, graph.getConfig()
        .getExecutionEnvironment());
    btgGraph = btgGraph.run(new BTGAlgorithm(this.maxIterations));
    DataSet<VD> btgLabeledVertices =
      btgGraph.getVertices().join(graph.getVertices())
        .where(new BTGKeySelector())
        .equalTo(new VertexKeySelector<VD>())
        .with(new BTGJoin<VD>());
    // create new graph
    LogicalGraph<GD, VD, ED> btgEPGraph = LogicalGraph
      .fromDataSets(btgLabeledVertices, graph.getEdges(), null,
        graph.getConfig());

    // create collection from result and return
    return new OverlapSplitBy<VD, ED, GD>(
      new LongListFromPropertyFunction<VD>(VERTEX_BTGIDS_PROPERTYKEY))
      .execute(btgEPGraph);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return BTG.class.getName();
  }
}
