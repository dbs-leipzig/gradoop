package org.gradoop.model.impl.operators.labelpropagation;

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
import org.gradoop.model.helper.KeySelectors;
import org.gradoop.model.helper.LongFromVertexFunction;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.SplitBy;
import org.gradoop.model.operators.UnaryGraphToCollectionOperator;

/**
 * LabelPropagation Graph to Collection Operator
 */
public class LabelPropagation<VD extends VertexData, ED extends EdgeData, GD
  extends GraphData> implements
  UnaryGraphToCollectionOperator<VD, ED, GD> {
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
  public GraphCollection<VD, ED, GD> execute(LogicalGraph<VD, ED, GD> epGraph) {
    DataSet<Vertex<Long, LabelPropagationValue>> vertices =
      epGraph.getGellyGraph().getVertices().map(
        new MapFunction<Vertex<Long, VD>, Vertex<Long,
          LabelPropagationValue>>() {
          @Override
          public Vertex<Long, LabelPropagationValue> map(
            Vertex<Long, VD> vertex) throws Exception {
            return new Vertex<>(vertex.getId(),
              new LabelPropagationValue(vertex.getId(), (Long) vertex.getValue()
                .getProperty(EPGLabelPropagationAlgorithm.CURRENT_VALUE)));
          }
        });
    DataSet<Edge<Long, NullValue>> edges = epGraph.getGellyGraph().getEdges()
      .map(new MapFunction<Edge<Long, ED>, Edge<Long, NullValue>>() {
        @Override
        public Edge<Long, NullValue> map(Edge<Long, ED> edge) throws Exception {
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

    DataSet<Vertex<Long, VD>> labeledVertices =
      graph.getVertices().join(epGraph.getGellyGraph().getVertices())
        .where(new LPKeySelector())
        .equalTo(new KeySelectors.VertexKeySelector<VD>())
        .with(new LPJoin<VD>());

    Graph<Long, VD, ED> gellyGraph = Graph
      .fromDataSet(labeledVertices, epGraph.getGellyGraph().getEdges(), env);
    LogicalGraph<VD, ED, GD> labeledGraph = LogicalGraph
      .fromGraph(gellyGraph, null, epGraph.getVertexDataFactory(),
        epGraph.getEdgeDataFactory(), epGraph.getGraphDataFactory());
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
   * JoinFunction over LPVertex id and EPVertex id
   */
  private static class LPJoin<VD extends VertexData> implements
    JoinFunction<Vertex<Long, LabelPropagationValue>, Vertex<Long, VD>,
      Vertex<Long, VD>> {
    @Override
    public Vertex<Long, VD> join(Vertex<Long, LabelPropagationValue> lpVertex,
      Vertex<Long, VD> epVertex) throws Exception {
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
  private static class LongFromProperty<VD extends VertexData> implements
    LongFromVertexFunction<VD> {
    /**
     * String propertyKey
     */
    String propertyKey;

    /**
     * Constructor
     *
     * @param propertyKey propertyKey for the p   * @param env
     *                    ExecutionEnvironment
     *                    roperty map
     */
    public LongFromProperty(String propertyKey) {
      this.propertyKey = propertyKey;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long extractLong(Vertex<Long, VD> vertex) {
      return (long) vertex.getValue().getProperties().get(propertyKey);
    }
  }
}
