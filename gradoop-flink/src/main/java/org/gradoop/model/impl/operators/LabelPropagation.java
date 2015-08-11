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
import org.gradoop.model.helper.LongFromVertexFunction;
import org.gradoop.model.impl.EPFlinkEdgeData;
import org.gradoop.model.impl.EPFlinkVertexData;
import org.gradoop.model.impl.EPGraph;
import org.gradoop.model.impl.EPGraphCollection;
import org.gradoop.model.operators.UnaryGraphToCollectionOperator;

import java.io.Serializable;

/**
 * LabelPropagation Graph to Collection Operator
 */
public class LabelPropagation implements UnaryGraphToCollectionOperator,
  Serializable {
  /**
   * Counter to define maximal Iteration for the Algorithm
   */
  private int maxIterations;
  /**
   * Value PropertyKey
   */
  private String propertyKey = "lpvertex.value";
  /**
   * Flink Execution Environment
   */
  private final ExecutionEnvironment env;

  /**
   * Constructor
   *
   * @param maxIterations Counter to define maximal Iteration for the Algorithm
   * @param propertyKey   PropertyKey of the EPVertex value
   * @param env           ExecutionEnvironment
   */
  public LabelPropagation(int maxIterations, String propertyKey,
    ExecutionEnvironment env) {
    this.maxIterations = maxIterations;
    this.propertyKey = propertyKey;
    this.env = env;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EPGraphCollection execute(EPGraph epGraph) throws Exception {
    DataSet<Vertex<Long, LabelPropagationValue>> vertices =
      epGraph.getGellyGraph().getVertices().map(
        new MapFunction<Vertex<Long, EPFlinkVertexData>, Vertex<Long,
          LabelPropagationValue>>() {
          @Override
          public Vertex<Long, LabelPropagationValue> map(
            Vertex<Long, EPFlinkVertexData> vertex) throws Exception {
            return new Vertex<>(vertex.getId(),
              new LabelPropagationValue(vertex.getId(), (Long) vertex.getValue()
                .getProperty(EPGLabelPropagationAlgorithm.CURRENT_VALUE)));
          }
        });
    DataSet<Edge<Long, NullValue>> edges = epGraph.getGellyGraph().getEdges()
      .map(
        new MapFunction<Edge<Long, EPFlinkEdgeData>, Edge<Long, NullValue>>() {
          @Override
          public Edge<Long, NullValue> map(
            Edge<Long, EPFlinkEdgeData> edge) throws Exception {
            return new Edge<>(edge.getSource(), edge.getTarget(),
              NullValue.getInstance());
          }
        });
    Graph<Long, LabelPropagationValue, NullValue> graph =
      Graph.fromDataSet(vertices, edges, env);
    try {
      graph = graph.run(new LabelPropagationAlgorithm(this.maxIterations));
    } catch (Exception e) {
      e.printStackTrace();
    }
    DataSet<Vertex<Long, EPFlinkVertexData>> labeledVertices =
      graph.getVertices().join(epGraph.getGellyGraph().getVertices())
        .where(new LPKeySelector()).equalTo(new VertexKeySelector())
        .with(new LPJoin());
    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> gellyGraph = Graph
      .fromDataSet(labeledVertices, epGraph.getGellyGraph().getEdges(), env);
    EPGraph labeledGraph = EPGraph.fromGraph(gellyGraph, null);
    LongFromProperty lfp = new LongFromProperty(propertyKey);
    SplitBy callByPropertyKey = new SplitBy(lfp, env);
    return callByPropertyKey.execute(labeledGraph);
  }

  /**
   * KeySelector class for LPVertex
   */
  private static class LPKeySelector implements
    KeySelector<Vertex<Long, LabelPropagationValue>, Long> {
    @Override
    public Long getKey(Vertex<Long, LabelPropagationValue> vertex) throws
      Exception {
      return vertex.getId();
    }
  }

  /**
   * KeySelector class for EPVertex
   */
  private static class VertexKeySelector implements
    KeySelector<Vertex<Long, EPFlinkVertexData>, Long> {
    @Override
    public Long getKey(Vertex<Long, EPFlinkVertexData> vex) throws Exception {
      return vex.getId();
    }
  }

  /**
   * JoinFunction over LPVertex id and EPVertex id
   */
  private static class LPJoin implements
    JoinFunction<Vertex<Long, LabelPropagationValue>, Vertex<Long,
      EPFlinkVertexData>, Vertex<Long, EPFlinkVertexData>> {
    @Override
    public Vertex<Long, EPFlinkVertexData> join(
      Vertex<Long, LabelPropagationValue> lpVertex,
      Vertex<Long, EPFlinkVertexData> epVertex) throws Exception {
      epVertex.getValue()
        .setProperty(EPGLabelPropagationAlgorithm.CURRENT_VALUE,
          lpVertex.getValue().getCurrentCommunity());
      return epVertex;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return "LabelPropagation";
  }

  /**
   * Class defining a mapping from vertex to value (long) of a property of this
   * vertex
   */
  private static class LongFromProperty implements LongFromVertexFunction {
    /**
     * String propertyKey
     */
    String propertyKey;

    /**
     * Constructor
     *
     * @param propertyKey propertyKey for the p   * @param env ExecutionEnvironment
roperty map
     */
    public LongFromProperty(String propertyKey) {
      this.propertyKey = propertyKey;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long extractLong(Vertex<Long, EPFlinkVertexData> vertex) {
      return (long) vertex.getValue().getProperties().get(propertyKey);
    }
  }
}
