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
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.datasetiteration.CheckIfVerticesToVisitLeftFilter;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.datasetiteration.DSIEdgeJoin;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.datasetiteration.DSIVertexJoin;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.datasetiteration.EdgeToSourceTargetTupleMap;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.datasetiteration.GetActiveTupleFilter;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.datasetiteration.GetSourceIdFromIterativeTuple;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.datasetiteration.GetUnvisitedTargetOrRandomVertexRichFlatMap;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.datasetiteration.IterativeTuple;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.datasetiteration.SetNextVertexActiveRichMap;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.datasetiteration.SourceTargetToSourceTargetsListGroupReduce;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.datasetiteration.UpdateVisitedOnIterativeSetRichMap;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.datasetiteration.VerticesWithSourceTargetsListFlatJoin;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Performs the RandomJump using a DataSetIteration (DSI).
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
public class RandomJumpDataSetIteration implements RandomJumpBase {

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
   * Creates an instance of RandomJumpDataSetIteration.
   *
   * @param maxIterations Value for maximum number of iterations for the algorithm
   * @param jumpProbability Probability for jumping to random vertex instead of walking to random
   *                       neighbor
   * @param percentageToVisit Relative amount of vertices to visit via walk or jump
   */
  public RandomJumpDataSetIteration(int maxIterations, double jumpProbability,
    double percentageToVisit) {
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
   * Runs a DataSetIteration (DSI) to compute the RandomJump. Annotates vertices and edges with a
   * {@code boolean} property, determining if a vertex or an edge was visited (@code true),
   * or not {@code false}.
   *
   * @param graph {@link LogicalGraph} for computation
   * @return {@link LogicalGraph} with annotated vertices and edges
   */
  public LogicalGraph execute(LogicalGraph graph) {

    // INITIALIZATION

    final String vertexIdsBroadcastSet = "vertexIds";
    final String nextActiveVertexBroadcastSet = "nextActiveVertex";
    final String currentVisitedCountBroadcastSet = "currentVisitedCount";

    long toVisitCount;
    try {
      toVisitCount = (long) Math.ceil((double) graph.getVertices().count() * percentageToVisit);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    DataSet<GradoopId> vertexIds = graph.getVertices().map(new Id<>());

    DataSet<Tuple2<GradoopId, List<IterativeTuple.EdgeType>>> sourceTargetsList = graph
      .getEdges().map(new EdgeToSourceTargetTupleMap())
      .groupBy(0)
      .reduceGroup(new SourceTargetToSourceTargetsListGroupReduce(jumpProbability));

    DataSet<IterativeTuple> iterativeTuples = graph.getVertices().leftOuterJoin(sourceTargetsList)
      .where(new Id<>()).equalTo(0)
      .with(new VerticesWithSourceTargetsListFlatJoin());

    DataSet<GradoopId> nextActiveVertex = iterativeTuples.first(1)
      .map(new GetSourceIdFromIterativeTuple());
    iterativeTuples = iterativeTuples
      .map(new SetNextVertexActiveRichMap(nextActiveVertexBroadcastSet))
      .withBroadcastSet(nextActiveVertex, nextActiveVertexBroadcastSet);

    // ITERATION HEAD

    IterativeDataSet<IterativeTuple> iterativeSet = iterativeTuples.iterate(maxIterations);

    // ITERATION BODY

    DataSet<IterativeTuple> activeTuple = iterativeSet.filter(new GetActiveTupleFilter());

    nextActiveVertex = activeTuple
      .flatMap(new GetUnvisitedTargetOrRandomVertexRichFlatMap(vertexIdsBroadcastSet))
      .withBroadcastSet(vertexIds, vertexIdsBroadcastSet);

    DataSet<IterativeTuple> updatedIterativeSet = iterativeSet
      .map(new UpdateVisitedOnIterativeSetRichMap(
        nextActiveVertexBroadcastSet, currentVisitedCountBroadcastSet, toVisitCount))
      .withBroadcastSet(nextActiveVertex, nextActiveVertexBroadcastSet)
      .withBroadcastSet(iterativeSet.sum(2), currentVisitedCountBroadcastSet);

    updatedIterativeSet = updatedIterativeSet
      .map(new SetNextVertexActiveRichMap(nextActiveVertexBroadcastSet))
      .withBroadcastSet(nextActiveVertex, nextActiveVertexBroadcastSet);

    DataSet<IterativeTuple> currentVisitedCount = updatedIterativeSet.sum(2)
      .filter(new CheckIfVerticesToVisitLeftFilter(toVisitCount));

    // ITERATION FOOTER

    DataSet<IterativeTuple> result = iterativeSet
      .closeWith(updatedIterativeSet, currentVisitedCount);

    // BUILD RESULT GRAPH

    DataSet<Vertex> annotatedVertices = graph.getVertices().join(result)
      .where(new Id<>()).equalTo(IterativeTuple::getSourceId)
      .with(new DSIVertexJoin(PROPERTY_KEY_VISITED));

    DataSet<Edge> annotatedEdges = graph.getEdges().leftOuterJoin(result)
      .where(new SourceId<>()).equalTo(IterativeTuple::getSourceId)
      .with(new DSIEdgeJoin(PROPERTY_KEY_VISITED));

    return graph.getConfig().getLogicalGraphFactory().fromDataSets(
      graph.getGraphHead(), annotatedVertices, annotatedEdges);
  }

  public String getName() {
    return RandomJumpDataSetIteration.class.getName();
  }
}
