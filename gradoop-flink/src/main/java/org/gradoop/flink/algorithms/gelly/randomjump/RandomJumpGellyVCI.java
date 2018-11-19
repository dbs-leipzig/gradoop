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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.pregel.VertexCentricConfiguration;
import org.apache.flink.graph.pregel.VertexCentricIteration;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.gelly.GradoopGellyAlgorithm;
import org.gradoop.flink.algorithms.gelly.functions.EdgeToGellyEdgeWithNullValue;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.gellyvci.VCIComputeFunction;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.gellyvci.VCIEdgeJoin;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.gellyvci.VCIVertexJoin;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.gellyvci.VCIVertexValue;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.gellyvci.VertexToGellyVertexWithVCIVertexValue;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Performs the RandomJump using Gellys {@link VertexCentricIteration} (VCI).
 * Uniformly at random picks a starting vertex and then simulates a RandomWalk on the graph,
 * where once visited edges are not used again. With a given {@link #jumpProbability}, or if the
 * walk ends in a sink, or if all outgoing edges of the current vertex were visited, randomly
 * jumps to any vertex in the graph and starts a new RandomWalk from there.
 * Unlike the RandomWalk algorithm, RandomJump does not have problems of getting stuck or not being
 * able to visit enough vertices. The algorithm converges when the maximum number of iterations
 * has been reached, or enough vertices has been visited (with the percentage of vertices to
 * visit given in {@link #percentageToVisit}).
 * Returns the initial graph with vertices and edges annotated by a boolean property named
 * {@value #PROPERTY_KEY_VISITED}, which is set to {@code true} if visited, or {@code false} if not.
 */
public class RandomJumpGellyVCI extends GradoopGellyAlgorithm<VCIVertexValue, NullValue>
  implements RandomJumpBase {

  /**
   * Value for maximum number of iterations for the algorithm
   */
  private final int maxIterations;

  /**
   * Probability for jumping to a random vertex instead of walking to a random neighbor
   */
  private final double jumpProbability;

  /**
   * Relative amount of vertices to visit via walk or jump
   */
  private final double percentageToVisit;

  /**
   * Creates an instance of RandomJumpGellyVCI.
   * Calls constructor of super-class.
   *
   * @param maxIterations Value for maximum number of iterations for the algorithm
   * @param jumpProbability Probability for jumping to random vertex instead of walking to random
   *                       neighbor
   * @param percentageToVisit Relative amount of vertices to visit via walk or jump
   */
  public RandomJumpGellyVCI(int maxIterations, double jumpProbability, double percentageToVisit) {
    super(new VertexToGellyVertexWithVCIVertexValue(),
      new EdgeToGellyEdgeWithNullValue());
    checkArgument(maxIterations > 0,
      "maxIterations must be greater than 0");
    checkArgument(jumpProbability >= 0d && jumpProbability <= 1d,
      "jumpProbability must be equal/greater than 0.0 and smaller/equal 1.0");
    checkArgument(percentageToVisit > 0d && percentageToVisit <= 1d,
      "percentageToVisit must be greater than 0.0 and smaller/equal 1.0");
    this.maxIterations = maxIterations;
    this.jumpProbability = jumpProbability;
    this.percentageToVisit = percentageToVisit;
  }

  /**
   * Executes the computation of the RandomJump. Annotates vertices and edges with a
   * {@code boolean} property, determining if a vertex or an edge was visited (@code true)
   * or not {@code false}.
   *
   * @param gellyGraph Gelly graph with initialized vertices
   * @return {@link LogicalGraph} with annotated vertices and edges
   * @throws Exception Thrown if the gelly algorithm fails
   */
  public LogicalGraph executeInGelly(Graph<GradoopId, VCIVertexValue, NullValue> gellyGraph)
    throws Exception {

    DataSet<GradoopId> randomVertexId = gellyGraph.getVertexIds().first(1);

    long verticesToVisit = (long) Math.ceil((double) gellyGraph.numberOfVertices() *
      percentageToVisit);

    final String startIdBroadcastSet = "startId";
    final String vertexIdsBroadcastSet = "vertexIds";

    VertexCentricConfiguration parameters = new VertexCentricConfiguration();
    parameters.addBroadcastSet(startIdBroadcastSet, randomVertexId);
    parameters.addBroadcastSet(vertexIdsBroadcastSet, gellyGraph.getVertexIds());

    Graph<GradoopId, VCIVertexValue, NullValue> resultGraph = gellyGraph
      .runVertexCentricIteration(new VCIComputeFunction(
        jumpProbability, verticesToVisit, startIdBroadcastSet, vertexIdsBroadcastSet), null,
        maxIterations, parameters);

    DataSet<Vertex> annotatedVertices = resultGraph.getVertices()
      .join(currentGraph.getVertices())
      .where(0).equalTo(new Id<>())
      .with(new VCIVertexJoin(PROPERTY_KEY_VISITED));

    DataSet<Edge> annotatedEdges = currentGraph.getEdges()
      .leftOuterJoin(resultGraph.getVertices())
      .where(new SourceId<>()).equalTo(0)
      .with(new VCIEdgeJoin(PROPERTY_KEY_VISITED));

    return currentGraph.getConfig().getLogicalGraphFactory().fromDataSets(
      currentGraph.getGraphHead(), annotatedVertices, annotatedEdges);
  }

  @Override
  public String getName() {
    return RandomJumpGellyVCI.class.getName();
  }
}
