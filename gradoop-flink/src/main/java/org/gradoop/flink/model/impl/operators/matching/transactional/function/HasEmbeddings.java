/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

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
