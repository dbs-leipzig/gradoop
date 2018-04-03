package org.gradoop.flink.algorithms.jaccardindex.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.Denominator;

import static org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.DEFAULT_JACCARD_EDGE_PROPERTY;

/**
 * COPIED WITH MINOR MODIFICATIONS FROM
 * {@link org.apache.flink.graph.library.similarity.JaccardIndex}
 * Compute the counts of shared and distinct neighbors. A two-path connecting
 * the vertices is emitted for each shared neighbor.
 */
public class ComputeScores implements GroupReduceFunction<Tuple3<GradoopId, GradoopId, IntValue>, Edge> {
  /**
   * The label for newly created result edges
   **/
  private final String edgeLabel;

  /**
   * Denominator for computing the score
   */
  private final Denominator denominator;

  public ComputeScores(String edgeLabel, Denominator denominator) {
    this.edgeLabel = edgeLabel;
    this.denominator = denominator;
  }

  @Override
  public void reduce(Iterable<Tuple3<GradoopId, GradoopId, IntValue>> values,
    Collector<Edge> out) throws Exception {
    int count = 0;
    Tuple3<GradoopId, GradoopId, IntValue> edge = null;


    for (Tuple3<GradoopId, GradoopId, IntValue> next : values) {
      edge = next;
      count += 1;
    }

    int denominator = computeDenominatorValue(edge.f2.getValue(), count);

    out.collect(createResultEdge(edge.f0, edge.f1, (double) count / denominator,
      edgeLabel));
    out.collect(createResultEdge(edge.f1, edge.f0, (double) count / denominator,
      edgeLabel));
  }

  /**
   * Creates a result edge with given source, target, value and edge label
   * @param source
   * @param target
   * @param value
   * @param edgeLabel
   * @return
   */
  private Edge createResultEdge(GradoopId source, GradoopId target, double value, String
    edgeLabel) {
    Edge edge = new Edge();
    edge.setId(GradoopId.get());
    edge.setSourceId(source);
    edge.setTargetId(target);
    edge.setProperty(DEFAULT_JACCARD_EDGE_PROPERTY, value);
    edge.setLabel(edgeLabel);
    return edge;
  }

  /**
   * Returns the denominator value in dependence of the set configuration
   * The number of common neighbors is equal to the sum of degrees of the vertices minus the count
   * of shared numbers, which are double-counted in the degree sum.
   *
   * In case of the maximum neighborhood size, the value is already stored in the sum variable
   *
   * @param sum
   * @param count
   * @return
   */
  private int computeDenominatorValue(int sum, int count) {
    if(denominator.equals(Denominator.UNION)) {
      return sum - count;
    } else {
      return sum;
    }
  }
}