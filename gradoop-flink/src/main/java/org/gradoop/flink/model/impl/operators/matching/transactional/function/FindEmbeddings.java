
package org.gradoop.flink.model.impl.operators.matching.transactional.function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.Embedding;
import org.gradoop.flink.model.impl.operators.matching.transactional.algorithm.PatternMatchingAlgorithm;
import org.gradoop.flink.model.impl.operators.matching.transactional.tuples.GraphWithCandidates;

import java.util.List;

/**
 * Mapping function that applies a custom pattern matching algorithm to
 * a GraphWithCandidates, representing a graph, its elements and their
 * candidates. Returns the found embeddings.
 */
public class FindEmbeddings
  implements FlatMapFunction<
  GraphWithCandidates,
  Tuple4<GradoopId, GradoopId, GradoopIdList, GradoopIdList>> {

  /**
   * The pattern matching algorithm.
   */
  private PatternMatchingAlgorithm algo;

  /**
   * The query string, input fo the pattern matching algorithm.
   */
  private String query;

  /**
   * Constructor
   * @param algo custom algorithm
   * @param query query string
   */
  public FindEmbeddings(PatternMatchingAlgorithm algo, String query) {
    this.algo = algo;
    this.query = query;
  }


  @Override
  public void flatMap(GraphWithCandidates graphWithCandidates,
    Collector<Tuple4<GradoopId, GradoopId, GradoopIdList, GradoopIdList>> collector) throws
    Exception {
    List<Embedding<GradoopId>> embeddings =
      this.algo.findEmbeddings(graphWithCandidates, this.query);

    for (Embedding<GradoopId> embedding : embeddings) {
      GradoopId newGraphId = GradoopId.get();
      collector.collect(new Tuple4<>(newGraphId,
        graphWithCandidates.f0,
        GradoopIdList.fromExisting(embedding.getVertexMapping()),
        GradoopIdList.fromExisting(embedding.getEdgeMapping())));
    }
  }
}
