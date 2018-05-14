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
package org.gradoop.flink.algorithms.jaccardindex;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.types.IntValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.algorithms.jaccardindex.functions.ComputeScores;
import org.gradoop.flink.algorithms.jaccardindex.functions.NeighborhoodTypeEdgeKeySelector;
import org.gradoop.flink.algorithms.jaccardindex.functions.GenerateGroupPairs;
import org.gradoop.flink.algorithms.jaccardindex.functions.GenerateGroupSpans;
import org.gradoop.flink.algorithms.jaccardindex.functions.GenerateGroups;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.operators.statistics.IncomingVertexDegrees;
import org.gradoop.flink.model.impl.operators.statistics.OutgoingVertexDegrees;
import org.gradoop.flink.model.impl.tuples.WithCount;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;
import static org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.Denominator.UNION;
import static org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.NeighborhoodType.IN;
import static org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.NeighborhoodType.OUT;

/**
 * Computes the Jaccard Similarity of all vertices that have at least one common neighbor.
 * Without further configuration the Jaccard Similarity of two vertices is defined as the number
 * of common (outgoing) neighbors (intersection of the neighborhoods) divided by the number of total
 * neighbors of both vertices (size of the union of the neighborhoods).
 * The definition of the neighborhood can be changed to regard vertices that are connected via
 * an incoming edge instead. The denominator may be changed towards the maximum size of both
 * neighborhoods.
 *
 * Results of the algorithm are stored in edge-pairs between each two vertices with
 * similarity > 0.
 *
 * The implementation of this  algorithm and its functions is based on the Gelly Jaccard Index
 * implementation but extends its configurability
 * @see <a href="https://ci.apache.org/projects/flink/flink-docs-release-1.3/api/java/org/apache/flink/graph/library/similarity/JaccardIndex.html">
 * Gelly Jaccard Index </a>
 *
 **/
public class JaccardIndex implements UnaryGraphToGraphOperator {

  /**
   * Group size for the quadratic expansion of neighbor pairs - not configurable
   **/
  public static final int DEFAULT_GROUP_SIZE = 64;

  /**
   * Default label for result edges
   **/
  private static final  String DEFAULT_JACCARD_EDGE_LABEL = "jaccardSimilarity";

  /**
   * The label for the result edges
   */
  private final String edgeLabel;

  /**
   * Type of vertex neighborhood to be considered by the algorithm
   */
  private final NeighborhoodType neighborhoodType;

  /**
   * Type of denominator to compute the score
   */
  private final Denominator denominator;

  /**
   * The different types of neighborhoods
   */
  public enum NeighborhoodType { IN, OUT }

  /**
   * The possible denominators
   */
  public enum Denominator { UNION, MAX }

  /**
   * Creates a new JaccardIndex with default configuration.
   */
  public JaccardIndex() {
    this(DEFAULT_JACCARD_EDGE_LABEL, OUT, UNION);
  }

  /**
   * Creates a new JaccardIndex with the given configuration.
   * @param edgeLabel the label for the edges storing results
   * @param neighborhoodType direction of neighborhood
   * @param denominator denominator for computing the score
   */
  public JaccardIndex(String edgeLabel, NeighborhoodType neighborhoodType, Denominator
    denominator) {
    this.edgeLabel = edgeLabel;
    this.neighborhoodType = neighborhoodType;
    this.denominator = denominator;
  }

  @Override
  public LogicalGraph execute(LogicalGraph inputGraph) {

    // VertexDegrees
    DataSet<Tuple3<GradoopId, GradoopId, Long>> edgesWithDegree =
      annotateEdgesWithDegree(inputGraph);

    // group span, source, target, degree(t/s)
    DataSet<Tuple4<IntValue, GradoopId, GradoopId, IntValue>> groupSpans =
      edgesWithDegree
        .groupBy(0)
        .sortGroup(1, Order.ASCENDING)
        .reduceGroup(new GenerateGroupSpans())
        .setParallelism(PARALLELISM_DEFAULT)
        .name("Generate group spans");

    // group, s, t, d(t)
    DataSet<Tuple4<IntValue, GradoopId, GradoopId, IntValue>> groups =
      groupSpans
        .rebalance()
        .setParallelism(PARALLELISM_DEFAULT)
        .name("Rebalance")
        .flatMap(new GenerateGroups())
        .setParallelism(PARALLELISM_DEFAULT)
        .name("Generate groups");

    DataSet<Tuple3<GradoopId, GradoopId, IntValue>> twoPaths = groups
      .groupBy(0, neighborhoodType.equals(IN) ? 1 : 2)
      .sortGroup(1, Order.ASCENDING)
      .reduceGroup(new GenerateGroupPairs(neighborhoodType, denominator))
      .name("Generate group pairs");

    DataSet<Edge> scoreEdges =
      twoPaths.groupBy(0, 1)
        .reduceGroup(new ComputeScores(edgeLabel, denominator))
        .name("Compute scores");

    DataSet<Edge> allEdges = scoreEdges.union(inputGraph.getEdges());

    return inputGraph.getConfig().getLogicalGraphFactory()
      .fromDataSets(inputGraph.getVertices(), allEdges);
  }

  /**
   * Returns the edges from the given logical graph annotated with either the degree of the
   * source or target vertex (depending on the neighborhood configuration).
   * @param inputGraph the input graph
   * @return edges with vertex degree as Tuple3<Source, Target, Degree>
   */
  private DataSet<Tuple3<GradoopId, GradoopId, Long>> annotateEdgesWithDegree(LogicalGraph
    inputGraph) {
    UnaryGraphToValueOperator<DataSet<WithCount<GradoopId>>> degreeOperator =
      getDegreeOperator(neighborhoodType);
    DataSet<WithCount<GradoopId>> degrees = degreeOperator.execute(inputGraph);

    return inputGraph
      .getEdges()
      .join(degrees)
      .where(new NeighborhoodTypeEdgeKeySelector(neighborhoodType))
      .equalTo(new KeySelector<WithCount<GradoopId>, GradoopId>() {
        @Override
        public GradoopId getKey(WithCount<GradoopId> value) {
          return value.getObject();
        }
      })
      .with(new JoinFunction<Edge, WithCount<GradoopId>, Tuple3<GradoopId, GradoopId, Long>>() {
        @Override
        public Tuple3<GradoopId, GradoopId, Long> join(Edge edge,
          WithCount<GradoopId> vertexDegree) {
          return new Tuple3<>(edge.getSourceId(), edge.getTargetId(), vertexDegree.getCount());
        }
      });
  }

  /**
   * Returns the appropriate vertex degree Operator depending on the given neighborhood type.
   * @param neighborhoodType the direction of the neighborhood to be considered by the algorithm
   * @return a vertice degree operator
   */
  private UnaryGraphToValueOperator<DataSet<WithCount<GradoopId>>> getDegreeOperator(
    NeighborhoodType neighborhoodType) {

    if (neighborhoodType.equals(IN)) {
      return new IncomingVertexDegrees();
    } else {
      return new OutgoingVertexDegrees();
    }
  }

  @Override
  public String getName() {
    return JaccardIndex.class.getName();
  }
}
