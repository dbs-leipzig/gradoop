package org.gradoop.model.impl.operators;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.gradoop.model.helper.LongListFromVertexFunction;
import org.gradoop.model.impl.EPFlinkEdgeData;
import org.gradoop.model.impl.EPFlinkVertexData;
import org.gradoop.model.impl.EPGraph;
import org.gradoop.model.impl.EPGraphCollection;
import org.gradoop.model.impl.OverlapSplitBy;
import org.gradoop.model.impl.operators.io.formats.BTGVertexType;
import org.gradoop.model.impl.operators.io.formats.BTGVertexValue;
import org.gradoop.model.operators.UnaryGraphToCollectionOperator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * BTG Graph to Collection Operator
 */
public class BTG implements UnaryGraphToCollectionOperator, Serializable {
  /**
   * BTGID PropertyKey
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
   * @param env           ExectuionEnvironment
   */
  public BTG(int maxIterations, ExecutionEnvironment env) {
    this.maxIterations = maxIterations;
    this.env = env;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EPGraphCollection execute(EPGraph graph) {
    DataSet<Vertex<Long, BTGVertexValue>> vertices =
      graph.getGellyGraph().getVertices().map(
        new MapFunction<Vertex<Long, EPFlinkVertexData>, Vertex<Long,
          BTGVertexValue>>() {
          @Override
          public Vertex<Long, BTGVertexValue> map(
            Vertex<Long, EPFlinkVertexData> epVertex) throws Exception {
            return new Vertex<Long, BTGVertexValue>(epVertex.getId(),
              getBTGValue(epVertex));
          }
        });
    DataSet<Edge<Long, NullValue>> edges = graph.getGellyGraph().getEdges().map(
      new MapFunction<Edge<Long, EPFlinkEdgeData>, Edge<Long, NullValue>>() {
        @Override
        public Edge<Long, NullValue> map(
          Edge<Long, EPFlinkEdgeData> edge) throws Exception {
          return new Edge<Long, NullValue>(edge.getSource(), edge.getTarget(),
            NullValue.getInstance());
        }
      });
    Graph<Long, BTGVertexValue, NullValue> btgGraph =
      Graph.fromDataSet(vertices, edges, env);
    try {
      btgGraph = btgGraph.run(new BTGAlgorithm(this.maxIterations));
    } catch (Exception e) {
      e.printStackTrace();
    }
    DataSet<Vertex<Long, EPFlinkVertexData>> btgLabeledVertices =
      btgGraph.getVertices().join(graph.getGellyGraph().getVertices())
        .where(new BTGKeySelector()).equalTo(new VertexKeySelector())
        .with(new BTGJoin());
    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> gellyBTGGraph = Graph
      .fromDataSet(btgLabeledVertices, graph.getGellyGraph().getEdges(), env);
    EPGraph btgEPGraph = EPGraph.fromGraph(gellyBTGGraph, null);
    LongListFromProperty lsfp =
      new LongListFromProperty(VERTEX_BTGIDS_PROPERTYKEY);
    OverlapSplitBy callByBtgIds = new OverlapSplitBy(lsfp, env);
    return callByBtgIds.execute(btgEPGraph);
  }

  /**
   * Method to return a new BTGVertexValue
   *
   * @param vertex epVertex
   * @return BTGVertexValue with data from the epVertex
   */
  private static BTGVertexValue getBTGValue(
    Vertex<Long, EPFlinkVertexData> vertex) {
    BTGVertexType vertexType = BTGVertexType.values()[Integer.valueOf(
      (String) vertex.getValue().getProperty(VERTEX_TYPE_PROPERTYKEY))];
    double vertexValue = Double.parseDouble(
      (String) vertex.getValue().getProperty(VERTEX_VALUE_PROPERTYKEY));
    List<Long> vertexBTGids = getBTGIDs(
      (String) vertex.getValue().getProperty(VERTEX_BTGIDS_PROPERTYKEY));
    return new BTGVertexValue(vertexType, vertexValue, vertexBTGids);
  }

  /**
   * Method to return the BTGIDs from a given epGraph
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
      for (int i = 0; i < btgIDArray.length; i++) {
        btgList.add(Long.parseLong(btgIDArray[i]));
      }
      return btgList;
    }
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
   * KeySelector class for the EPVertex
   */
  private static class VertexKeySelector implements
    KeySelector<Vertex<Long, EPFlinkVertexData>, Long> {
    @Override
    public Long getKey(Vertex<Long, EPFlinkVertexData> epVertex) throws
      Exception {
      return epVertex.getId();
    }
  }

  /**
   * JoinFunction over VertexIDs
   */
  private static class BTGJoin implements
    JoinFunction<Vertex<Long, BTGVertexValue>, Vertex<Long,
      EPFlinkVertexData>, Vertex<Long, EPFlinkVertexData>> {
    @Override
    public Vertex<Long, EPFlinkVertexData> join(
      Vertex<Long, BTGVertexValue> btgVertex,
      Vertex<Long, EPFlinkVertexData> epVertex) throws Exception {
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
    return null;
  }

  /**
   * Class defining a mapping from vertex to value (long) of a property of this
   * vertex
   */
  private static class LongListFromProperty implements
    LongListFromVertexFunction {
    /**
     * String propertyKey
     */
    String propertyKey;

    /**
     * Constructor
     *
     * @param propertyKey propertyKey for the property map
     */
    public LongListFromProperty(String propertyKey) {
      this.propertyKey = propertyKey;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Long> extractLongList(Vertex<Long, EPFlinkVertexData> vertex) {
      return (List<Long>) vertex.getValue().getProperty(propertyKey);
    }
  }
}
