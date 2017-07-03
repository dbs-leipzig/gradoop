
package org.gradoop.flink.model.impl.operators.matching.transactional.function;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.transactional.algorithm.PatternMatchingAlgorithm;
import org.gradoop.flink.model.impl.operators.matching.transactional.tuples.GraphWithCandidates;

/**
 * Mapping function that applies a custom pattern matching algorithm to
 * a GraphWithCandidates, representing a graph, its elements and their
 * candidates. Returns true, iff the graph contained the pattern.
 */
@FunctionAnnotation.ForwardedFields("f0")
public class HasEmbeddings implements
  MapFunction<GraphWithCandidates, Tuple2<GradoopId, Boolean>> {

  /**
   * The pattern matching algorithm.
   */
  private PatternMatchingAlgorithm algorithm;

  /**
   * The query string, input fo the pattern matching algorithm.
   */
  private String query;

  /**
   * Reduce instantiations
   */
  private Tuple2<GradoopId, Boolean> reuseTuple;

  /**
   * Constructor
   *
   * @param algorithm the pattern matching algorithm
   * @param query     the query string
   */
  public HasEmbeddings(PatternMatchingAlgorithm algorithm, String query) {
    this.reuseTuple = new Tuple2<>();
    this.algorithm = algorithm;
    this.query = query;
  }

  @Override
  public Tuple2<GradoopId, Boolean> map(GraphWithCandidates transaction) throws
    Exception {
    this.reuseTuple.f0 = transaction.getGraphId();
    this.reuseTuple.f1 = algorithm.hasEmbedding(transaction, query);
    return reuseTuple;
  }
}
