package org.gradoop.model.impl.operators.btg;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.gradoop.model.EdgeData;
import org.gradoop.model.GraphData;
import org.gradoop.model.VertexData;
import org.gradoop.model.helper.UnaryFunction;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.OverlapSplitBy;
import org.gradoop.model.operators.UnaryGraphToCollectionOperator;

import java.util.ArrayList;
import java.util.List;

/**
 * BTG Graph to Collection Operator.
 *
 * Encapsulates {@link BTGAlgorithm} in a Gradoop Operator.
 *
 * @param <VD> VertexData
 * @param <ED> EdgeData
 * @param <GD> GraphData
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
  private static final String VERTEX_TYPE_PROPERTYKEY = "vertex.btgtype";
  /**
   * BTGValue PropertyKey
   */
  private static final String VERTEX_VALUE_PROPERTYKEY = "vertex.btgvalue";
  /**
   * Counter to define maximal Iteration for the Algorithm
   */
  private int maxIterations;
  /**
   * Flink Execution Environment
   */
  private final ExecutionEnvironment env;

  /**
   * Constructor
   *
   * @param maxIterations int defining maximal step counter
   * @param env           ExecutionEnvironment
   */
  public BTG(int maxIterations, ExecutionEnvironment env) {
    this.maxIterations = maxIterations;
    this.env = env;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<VD, ED, GD> execute(
    final LogicalGraph<VD, ED, GD> logicalGraph) throws Exception {
    DataSet<Vertex<Long, BTGVertexValue>> vertices =
      logicalGraph.getVertices()
        .map(new CreateBTGVertexValueMapFunction<VD>());
    DataSet<Edge<Long, NullValue>> edges =
      logicalGraph.getEdges()
        .map(new CreateEdgesMapFunction<ED>());
    Graph<Long, BTGVertexValue, NullValue> btgGraph =
      Graph.fromDataSet(vertices, edges, env);
    btgGraph = btgGraph.run(new BTGAlgorithm(this.maxIterations));
    DataSet<Vertex<Long, VD>> btgLabeledVertices =
      btgGraph.getVertices().join(logicalGraph.getVertices())
        .where(new BTGKeySelector()).equalTo(new VertexKeySelector<VD>())
        .with(new BTGJoin<VD>());
    // create new graph
    LogicalGraph<VD, ED, GD> btgEPGraph = LogicalGraph
      .fromDataSets(btgLabeledVertices,
        logicalGraph.getEdges(),
        null,
        logicalGraph.getVertexDataFactory(),
        logicalGraph.getEdgeDataFactory(),
        logicalGraph.getGraphDataFactory());
    LongListFromProperty<VD> lsfp =
      new LongListFromProperty<>(VERTEX_BTGIDS_PROPERTYKEY);
    OverlapSplitBy<VD, ED, GD> callByBtgIds = new OverlapSplitBy<>(lsfp, env);
    return callByBtgIds.execute(btgEPGraph);
  }

  /**
   * KeySelector class for the BTGVertex
   */
  private static class BTGKeySelector implements
    KeySelector<Vertex<Long, BTGVertexValue>, Long> {
    @Override
    public Long getKey(Vertex<Long, BTGVertexValue> btgVertex) throws
      Exception {
      return btgVertex.getId();
    }
  }

  /**
   * Mapper class to create BTGVertex values
   *
   * @param <VD> VertexData
   */
  private static class CreateBTGVertexValueMapFunction<VD extends VertexData>
    implements
    MapFunction<Vertex<Long, VD>, Vertex<Long, BTGVertexValue>> {
    /**
     * {@inheritDoc}
     */
    @Override
    public Vertex<Long, BTGVertexValue> map(
      Vertex<Long, VD> logicalVertex) throws Exception {
      BTGVertexValue btgValue = createNewVertexValue(logicalVertex);
      return new Vertex<>(logicalVertex.getId(), btgValue);
    }

    /**
     * Method to create a new BTG vertex value from a given vertex
     *
     * @param logicalVertex actual vertex
     * @return BTGVertexValue
     */
    private BTGVertexValue createNewVertexValue(
      Vertex<Long, VD> logicalVertex) {
      BTGVertexType type = BTGVertexType.values()[Integer.parseInt(
        (String) logicalVertex.getValue()
          .getProperty(VERTEX_TYPE_PROPERTYKEY))];
      double value = Double.parseDouble((String) logicalVertex.getValue()
        .getProperty(VERTEX_VALUE_PROPERTYKEY));
      List<Long> btgIDs = getBTGIDs((String) logicalVertex.getValue()
        .getProperty(VERTEX_BTGIDS_PROPERTYKEY));
      return new BTGVertexValue(type, value, btgIDs);
    }

    /**
     * Method to return the BTG IDs from a given epGraph
     *
     * @param btgIDs String of BTGIDs
     * @return List of BTGIDs
     */
    private static List<Long> getBTGIDs(String btgIDs) {
      if (btgIDs.length() == 0) {
        return new ArrayList<>();
      } else {
        List<Long> btgList = new ArrayList<>();
        String[] btgIDArray = btgIDs.split(",");
        for (String aBtgIDArray : btgIDArray) {
          btgList.add(Long.parseLong(aBtgIDArray));
        }
        return btgList;
      }
    }
  }

  /**
   * MapFunction class to create edges for BTG Algorithm
   *
   * @param <ED> EdgeData
   */
  private static class CreateEdgesMapFunction<ED extends EdgeData> implements
    MapFunction<Edge<Long, ED>, Edge<Long, NullValue>> {
    @Override
    public Edge<Long, NullValue> map(Edge<Long, ED> edge) throws Exception {
      return new Edge<>(edge.getSource(), edge.getTarget(),
        NullValue.getInstance());
    }
  }

  /**
   * KeySelector class for the EPVertex
   */
  private static class VertexKeySelector<VD extends VertexData> implements
    KeySelector<Vertex<Long, VD>, Long> {
    @Override
    public Long getKey(Vertex<Long, VD> epVertex) throws Exception {
      return epVertex.getId();
    }
  }

  /**
   * JoinFunction over VertexIDs
   */
  private static class BTGJoin<VD extends VertexData> implements
    JoinFunction<Vertex<Long, BTGVertexValue>, Vertex<Long, VD>, Vertex<Long,
      VD>> {
    @Override
    public Vertex<Long, VD> join(Vertex<Long, BTGVertexValue> btgVertex,
      Vertex<Long, VD> epVertex) throws Exception {
      epVertex.getValue().setProperty(VERTEX_TYPE_PROPERTYKEY,
        btgVertex.getValue().getVertexType());
      epVertex.getValue().setProperty(VERTEX_VALUE_PROPERTYKEY,
        btgVertex.getValue().getVertexValue());
      epVertex.getValue().setProperty(VERTEX_BTGIDS_PROPERTYKEY,
        btgVertex.getValue().getGraphs());
      return epVertex;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return "btg";
  }

  /**
   * Class defining a mapping from vertex to value (long) of a property of this
   * vertex
   */
  private static class LongListFromProperty<VD extends VertexData> implements
    UnaryFunction<Vertex<Long, VD>, List<Long>> {
    /**
     * String propertyKey
     */
    private String propertyKey;

    /**
     * Constructor
     *
     * @param propertyKey propertyKey for the property map
     */
    public LongListFromProperty(String propertyKey) {
      this.propertyKey = propertyKey;
    }

    /**
     * use negative values
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public List<Long> execute(Vertex<Long, VD> vertex) throws Exception {
      return (List<Long>) vertex.getValue().getProperty(propertyKey);
    }
  }
}
