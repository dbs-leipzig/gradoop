package org.gradoop.model.impl.algorithms.btg;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.VertexData;
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
public class BTG<VD extends VertexData, ED extends EdgeData, GD extends
  GraphData> implements
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
  public GraphCollection<VD, ED, GD> execute(
    final LogicalGraph<VD, ED, GD> logicalGraph) throws Exception {
    DataSet<Vertex<Long, BTGVertexValue>> vertices =
      logicalGraph.getVertices().map(new VertexToBTGVertexMapper<VD>());
    DataSet<Edge<Long, NullValue>> edges =
      logicalGraph.getEdges().map(new EdgeToBTGEdgeMapper<ED>());
    Graph<Long, BTGVertexValue, NullValue> btgGraph =
      Graph.fromDataSet(vertices, edges, logicalGraph.getConfig()
        .getExecutionEnvironment());
    btgGraph = btgGraph.run(new BTGAlgorithm(this.maxIterations));
    DataSet<VD> btgLabeledVertices =
      btgGraph.getVertices().join(logicalGraph.getVertices())
        .where(new BTGKeySelector())
        .equalTo(new VertexKeySelector<VD>())
        .with(new BTGJoin<VD>());
    // create new graph
    LogicalGraph<VD, ED, GD> btgEPGraph = LogicalGraph
      .fromDataSets(btgLabeledVertices,
        logicalGraph.getEdges(),
        null,
       logicalGraph.getConfig());

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
