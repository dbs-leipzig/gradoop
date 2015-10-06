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
import org.gradoop.model.helper.UnaryFunction;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.SplitBy;
import org.gradoop.model.operators.UnaryGraphToCollectionOperator;

/**
 * LabelPropagation Graph to Collection Operator.
 *
 * Encapsulates {@link LabelPropagationAlgorithm} in a Gradoop operator.
 *
 * @param <VD> VertexData contains information about the vertex
 * @param <ED> EdgeData contains information about all edges of the vertex
 * @param <GD> GraphData contains information about the graphs of the vertex
 * @see LabelPropagationAlgorithm
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
  public GraphCollection<VD, ED, GD> execute(
    LogicalGraph<VD, ED, GD> epGraph) throws Exception {
    DataSet<Vertex<Long, LabelPropagationValue>> vertices =
      epGraph.getVertices().map(
        new MapFunction<Vertex<Long, VD>, Vertex<Long,
          LabelPropagationValue>>() {
          @Override
          public Vertex<Long, LabelPropagationValue> map(
            Vertex<Long, VD> vertex) throws Exception {
            return new Vertex<>(vertex.getId(),
              new LabelPropagationValue(vertex.getId(), vertex.getId()));
          }
        });
    DataSet<Edge<Long, NullValue>> edges = epGraph.getEdges()
      .map(new MapFunction<Edge<Long, ED>, Edge<Long, NullValue>>() {
        @Override
        public Edge<Long, NullValue> map(Edge<Long, ED> edge) throws Exception {
          return new Edge<>(edge.getSource(), edge.getTarget(),
            NullValue.getInstance());
        }
      });
    Graph<Long, LabelPropagationValue, NullValue> graph =
      Graph.fromDataSet(vertices, edges, env);
    graph = graph.run(new LabelPropagationAlgorithm(this.maxIterations));
    DataSet<Vertex<Long, VD>> labeledVertices =
      graph.getVertices().join(epGraph.getVertices())
        .where(new LPKeySelector())
        .equalTo(new KeySelectors.VertexKeySelector<VD>())
        .with(new LPJoin<VD>());

    LogicalGraph<VD, ED, GD> labeledGraph = LogicalGraph
      .fromDataSets(labeledVertices,
        epGraph.getEdges(),
        null,
        epGraph.getVertexDataFactory(),
        epGraph.getEdgeDataFactory(),
        epGraph.getGraphDataFactory());
    LongFromProperty<VD> lfp = new LongFromProperty<>(propertyKey);
    SplitBy<VD, ED, GD> callByPropertyKey = new SplitBy<>(lfp, env);
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
        .setProperty(EPGMLabelPropagationAlgorithm.CURRENT_VALUE,
          lpVertex.getValue().getCurrentCommunity());
      return epVertex;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return "labelpropagation";
  }

  /**
   * Class defining a mapping from vertex to value (long) of a property of this
   * vertex
   */
  private static class LongFromProperty<VD extends VertexData> implements
    UnaryFunction<Vertex<Long, VD>, Long> {
    /**
     * String propertyKey
     */
    private String propertyKey;

    /**
     * Constructor
     *
     * @param propertyKey propertyKey for the property map
     */
    public LongFromProperty(String propertyKey) {
      this.propertyKey = propertyKey;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long execute(Vertex<Long, VD> entity) throws Exception {
      return ((Long) entity.getValue().getProperty(propertyKey) + 1) * -1;
    }
  }
}
