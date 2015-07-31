package org.gradoop.model.impl.operators;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
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
   * Maximal Iterations for LabelPropagationAlgorithm
   */
  private int maxIterations;
  /**
   * Value PropertyKey
   */
  private String propertyKey;
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
  public LabelPropagation(int maxIterations, String propertyKey,
    ExecutionEnvironment env) {
    this.maxIterations = maxIterations;
    this.propertyKey = propertyKey;
    this.env = env;
  }

  /**
   * {@inheritDoc }
   */
  @Override
  public EPGraphCollection execute(EPGraph epGraph) {
    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> graph =
      epGraph.getGellyGraph();
    try {
      graph = graph.run(new LabelPropagationAlgorithm(this.maxIterations));
    } catch (Exception e) {
      e.printStackTrace();
    }
    EPGraph labeledGraph = EPGraph.fromGraph(graph, null, env);
    LongFromProperty lfp = new LongFromProperty(propertyKey);
    SplitBy callByPropertyKey = new SplitBy(lfp, env);
    return callByPropertyKey.execute(labeledGraph);
  }

  /**
   * {@inheritDoc }
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
     * @param propertyKey propertyKey for the property map
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
