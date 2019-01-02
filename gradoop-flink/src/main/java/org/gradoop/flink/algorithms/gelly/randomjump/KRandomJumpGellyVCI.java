/*
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
package org.gradoop.flink.algorithms.gelly.randomjump;

import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.VertexCentricConfiguration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.gelly.BaseGellyAlgorithm;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.EPGMEdgeWithGellyEdgeIdJoin;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.GellyVertexWithEPGMVertexJoin;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.GellyVertexWithLongIdToGradoopIdJoin;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.GetVisitedGellyEdgeLongIdsFlatMap;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.GetVisitedSourceTargetIdsFlatMap;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.LongIdToGellyVertexWithVCIValueMap;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.LongIdTupleToGellyEdgeWithLongValueJoin;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.LongIdWithEdgeToTupleJoin;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.ReplaceTargetWithLongIdJoin;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.VCIComputeFunction;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.VCIVertexValue;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.VertexWithVisitedSourceTargetIdJoin;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.VisitedGellyEdgesWithLongIdToGradoopIdJoin;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;

import java.util.HashSet;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Performs the RandomJump using Gellys VertexCentricIteration (VCI).
 * Uniformly at random picks {@link #k} starting vertices and then simulates a RandomWalk for each
 * starting vertex on the graph, where once visited edges are not used again. For each walker,
 * with a given {@link #jumpProbability}, or if the walk ends in a sink, or if all outgoing edges
 * of the current vertex were visited, randomly jumps to any vertex in the graph and starts a new
 * RandomWalk from there.
 * Unlike the RandomWalk algorithm, RandomJump does not have problems of getting stuck or not being
 * able to visit enough vertices. The algorithm converges when the maximum number of iterations
 * has been reached, or enough vertices have been visited (with the percentage of vertices to
 * visit at least given in {@link #percentageToVisit}).
 * Returns the initial graph with vertices and edges annotated by a boolean property named
 * {@value #PROPERTY_KEY_VISITED}, which is set to {@code true} if visited, or {@code false} if not.
 */
public class KRandomJumpGellyVCI
  extends BaseGellyAlgorithm<Long, VCIVertexValue, Long, LogicalGraph> {

  /**
   * Key to access the property value determining if a vertex or edge was visited
   */
  public static final String PROPERTY_KEY_VISITED = "visited";

  /**
   * The graph used in {@link KRandomJumpGellyVCI#execute(LogicalGraph)}.
   */
  protected LogicalGraph currentGraph;

  /**
   * Number of starting vertices
   */
  private final int k;

  /**
   * Value for maximum number of iterations for the algorithm
   */
  private final int maxIterations;

  /**
   * Probability for jumping to a random vertex instead of walking to a random neighbor
   */
  private final double jumpProbability;

  /**
   * Relative amount of vertices to visit at least
   */
  private final double percentageToVisit;

  /**
   * DataSet holding the mapping for a long index to its vertex gradoop id
   */
  private DataSet<Tuple2<Long, GradoopId>> indexToVertexIdMap;

  /**
   * DataSet holding the mapping for a long index to its edge gradoop id
   */
  private DataSet<Tuple2<Long, GradoopId>> indexToEdgeIdMap;

  /**
   * Creates an instance of KRandomJumpGellyVCI.
   *
   * @param k Number of starting vertices
   * @param maxIterations Value for maximum number of iterations for the algorithm
   * @param jumpProbability Probability for jumping to random vertex instead of walking to random
   *                       neighbor
   * @param percentageToVisit Relative amount of vertices to visit at least
   */
  public KRandomJumpGellyVCI(int k, int maxIterations, double jumpProbability,
    double percentageToVisit) {
    checkArgument(k >= 1,
      "at least 1 starting vertex is needed, k must be equal or greater 1");
    checkArgument(maxIterations > 0,
      "maxIterations must be greater than 0");
    checkArgument(jumpProbability >= 0d && jumpProbability <= 1d,
      "jumpProbability must be equal/greater than 0.0 and smaller/equal 1.0");
    checkArgument(percentageToVisit > 0d && percentageToVisit <= 1d,
      "percentageToVisit must be greater than 0.0 and smaller/equal 1.0");
    this.k = k;
    this.maxIterations = maxIterations;
    this.jumpProbability = jumpProbability;
    this.percentageToVisit = percentageToVisit;
  }

  @Override
  public Graph<Long, VCIVertexValue, Long> transformToGelly(LogicalGraph graph) {
    this.currentGraph = graph;

    indexToVertexIdMap = DataSetUtils.zipWithIndex(graph.getVertices().map(new Id<>()));
    indexToEdgeIdMap = DataSetUtils.zipWithIndex(graph.getEdges().map(new Id<>()));

    DataSet<Vertex<Long, VCIVertexValue>> vertices = indexToVertexIdMap
      .map(new LongIdToGellyVertexWithVCIValueMap());

    DataSet<Edge<Long, Long>> edges = graph.getEdges()
      .join(indexToVertexIdMap)
      .where(new SourceId<>()).equalTo(1)
      .with(new LongIdWithEdgeToTupleJoin())
      .join(indexToVertexIdMap)
      .where(1).equalTo(1)
      .with(new ReplaceTargetWithLongIdJoin())
      .join(indexToEdgeIdMap)
      .where(2).equalTo(1)
      .with(new LongIdTupleToGellyEdgeWithLongValueJoin());

    return Graph.fromDataSet(vertices, edges, graph.getConfig().getExecutionEnvironment());
  }

  @Override
  public LogicalGraph executeInGelly(Graph<Long, VCIVertexValue, Long> gellyGraph)
    throws Exception {

    Set<Long> randomStartIndices = new HashSet<>();
    while (randomStartIndices.size() < k) {
      long randomLongInBounds = (long) (Math.random() * (gellyGraph.numberOfVertices() - 1L));
      randomStartIndices.add(randomLongInBounds);
    }
    DataSet<Long> startIndices = currentGraph.getConfig().getExecutionEnvironment()
      .fromCollection(randomStartIndices);

    long verticesToVisit = (long) Math.ceil((double) gellyGraph.numberOfVertices() *
      percentageToVisit);

    final String startIndicesBroadcastSet = "startIndices";
    final String vertexIndicesBroadcastSet = "vertexIndices";
    final String visitedVerticesAggregatorName = "visitedVerticesAggregator";

    VertexCentricConfiguration parameters = new VertexCentricConfiguration();
    parameters.addBroadcastSet(startIndicesBroadcastSet, startIndices);
    parameters.addBroadcastSet(vertexIndicesBroadcastSet,
      indexToVertexIdMap.map(new Value0Of2<>()));
    parameters.registerAggregator(visitedVerticesAggregatorName, new LongSumAggregator());

    Graph<Long, VCIVertexValue, Long> resultGraph = gellyGraph.runVertexCentricIteration(
      new VCIComputeFunction(jumpProbability, verticesToVisit, startIndicesBroadcastSet,
        vertexIndicesBroadcastSet, visitedVerticesAggregatorName),
        null, maxIterations, parameters);

    DataSet<GradoopId> visitedGellyEdgeIds = resultGraph.getVertices()
      .flatMap(new GetVisitedGellyEdgeLongIdsFlatMap())
      .join(indexToEdgeIdMap)
      .where("*").equalTo(0)
      .with(new VisitedGellyEdgesWithLongIdToGradoopIdJoin());

    DataSet<org.gradoop.common.model.impl.pojo.Edge> annotatedEdges = currentGraph.getEdges()
      .leftOuterJoin(visitedGellyEdgeIds)
      .where(new Id<>()).equalTo("*")
      .with(new EPGMEdgeWithGellyEdgeIdJoin(PROPERTY_KEY_VISITED));

    DataSet<GradoopId> visitedSourceTargetIds = annotatedEdges
      .flatMap(new GetVisitedSourceTargetIdsFlatMap(PROPERTY_KEY_VISITED)).distinct();

    DataSet<org.gradoop.common.model.impl.pojo.Vertex> annotatedVertices = resultGraph.getVertices()
      .join(indexToVertexIdMap)
      .where(0).equalTo(0)
      .with(new GellyVertexWithLongIdToGradoopIdJoin())
      .join(currentGraph.getVertices())
      .where(0).equalTo(new Id<>())
      .with(new GellyVertexWithEPGMVertexJoin(PROPERTY_KEY_VISITED));

    annotatedVertices = annotatedVertices.leftOuterJoin(visitedSourceTargetIds)
      .where(new Id<>()).equalTo("*")
      .with(new VertexWithVisitedSourceTargetIdJoin(PROPERTY_KEY_VISITED));

    return currentGraph.getConfig().getLogicalGraphFactory().fromDataSets(
      currentGraph.getGraphHead(), annotatedVertices, annotatedEdges);
  }

  @Override
  public String getName() {
    return KRandomJumpGellyVCI.class.getName();
  }
}
