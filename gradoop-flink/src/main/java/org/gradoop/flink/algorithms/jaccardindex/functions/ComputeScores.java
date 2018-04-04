/**
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gradoop.flink.algorithms.jaccardindex.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.Denominator;

/**
 * COPIED WITH SOME MODIFICATIONS FROM
 * {@link org.apache.flink.graph.library.similarity.JaccardIndex}
 * Compute the counts of shared and distinct neighbors. A two-path connecting
 * the vertices is emitted for each shared neighbor.
 */
public class ComputeScores implements GroupReduceFunction<Tuple3<GradoopId, GradoopId, IntValue>, Edge> {

  /**
   * Default Key for Result Edges - not configurable
   **/
  private static final String DEFAULT_JACCARD_EDGE_PROPERTY = "value";

  /**
   * The label for newly created result edges
   **/
  private final String edgeLabel;

  /**
   * Denominator for computing the score
   */
  private final Denominator denominator;

  /**
   * Creates a new ComputeScores instance with the given configuration
   * @param edgeLabel label for the new edges that hold the results
   * @param denominator denominator to compute the results
   */
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

    int denominatorValue = computeDenominatorValue(edge.f2.getValue(), count);

    out.collect(createResultEdge(edge.f0, edge.f1, (double) count / denominatorValue,
      edgeLabel));
    out.collect(createResultEdge(edge.f1, edge.f0, (double) count / denominatorValue,
      edgeLabel));
  }

  /**
   * Creates a result edge with given source, target, value and edge label
   * @param source GradoopId of the source vertex
   * @param target GradoopId of the target vertex
   * @param value Jaccard Similarity of both vertices
   * @param edgeLabel the label for result edges
   * @return the newly created edge
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
   * of shared neeghbors, which are double-counted in the degree sum.
   *
   * In case of the maximum neighborhood size, the value is already stored in the sum variable
   *
   * @param sum sum of both edge degrees or maximum, depending on configuration
   * @param count number of shared neighbors
   * @return denominator value
   */
  private int computeDenominatorValue(int sum, int count) {
    if (denominator.equals(Denominator.UNION)) {
      return sum - count;
    } else {
      return sum;
    }
  }
}
