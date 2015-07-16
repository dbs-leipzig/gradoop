package org.gradoop.model.impl.operators;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
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
  Serializable{
  /**
   * Maximal Iterations for LabelPropagationAlgorithm
   */
  private int maxIterations;
  /**
   *
   */
  private String propertyKey;
  /**
   * Flink Execution Environment
   */
  private final ExecutionEnvironment env;
  /**
   *
   */
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
    SplitBy callByPropertyKey =
      new SplitBy (propertyKey, env);
    return callByPropertyKey.execute(labeledGraph);
  }

  @Override
  public String getName() {
    return "LabelPropagation";
  }
}
