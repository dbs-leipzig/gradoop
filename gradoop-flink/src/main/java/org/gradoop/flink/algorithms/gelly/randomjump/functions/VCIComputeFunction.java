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
package org.gradoop.flink.algorithms.gelly.randomjump.functions.gellyvci;

import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageIterator;
import org.gradoop.flink.algorithms.gelly.randomjump.KRandomJumpGellyVCI;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * <pre>
 * Compute function for {@link KRandomJumpGellyVCI}.
 * Vertex values are of type {@link VCIVertexValue}, with
 *  - f0: {@code Boolean} set to {@code true} if the vertex was visited, {@code false} otherwise
 *  - f1: {@code List<Long>} containing all ids from already visited outgoing edges
 *
 * A message from one vertex to another is considered a walk resp. a jump to this other vertex.
 * The message is of type {@code Long}, and contains the vertices unique long id, to distinguish
 * between different sender.
 * </pre>
 */
public class VCIComputeFunction extends ComputeFunction<Long, VCIVertexValue, Long, Long> {

  /**
   * Probability for jumping to a random vertex instead of walking to a random neighbor
   */
  private final double jumpProbability;

  /**
   * Number of vertices to visit via walk or jump
   */
  private final long verticesToVisit;

  /**
   * Name of the broadcast set containing the unique ids for the k vertices to start the RandomJump
   */
  private final String startIdsBroadcastSet;

  /**
   * List with the unique ids for the k starting vertices
   */
  private List startIds;

  /**
   * Name of the broadcast set containing the graphs vertex ids
   */
  private final String vertexUniqueLongIdsBroadcastSet;

  /**
   * List containing the graphs vertex unique long ids
   */
  private List vertexUniqueIds;

  /**
   * Name for the LongSumAggregator used for counting the visited vertices
   */
  private String visitedVerticesAggregatorName;

  /**
   * The LongSumAggregator used for counting the visited vertices
   */
  private LongSumAggregator visitedVerticesAggregator;

  /**
   * Flag to stop the iteration, if enough vertices are visited
   */
  private boolean stopIteration;

  /**
   * Keeping track of the currently visited vertices at the beginning at each superstep
   */
  private long currentVisitedCount;

  /**
   * Random generator to obtain random neighbors and vertices
   */
  private final Random random;

  /**
   * Creates an instance of VCIComputeFunction
   *
   * @param jumpProbability Probability for jumping to random vertex instead of walking to random
   *                        neighbor
   * @param verticesToVisit Number of vertices to visit via walk or jump
   * @param startIdsBroadcastSet Name of the broadcast set containing the Id of the vertex to
   *                                start the RandomJump
   * @param vertexUniqueLongIdsBroadcastSet Name of the broadcast set containing the unique long ids of
   *                                    all vertices
   */
  public VCIComputeFunction(double jumpProbability, long verticesToVisit,
    String startIdsBroadcastSet, String vertexUniqueLongIdsBroadcastSet,
    String visitedVerticesAggregatorName) {
    this.jumpProbability = jumpProbability;
    this.verticesToVisit = verticesToVisit;
    this.startIdsBroadcastSet = startIdsBroadcastSet;
    this.vertexUniqueLongIdsBroadcastSet = vertexUniqueLongIdsBroadcastSet;
    this.visitedVerticesAggregatorName = visitedVerticesAggregatorName;
    this.random = new Random();
    this.stopIteration = false;
  }

  /**
   * {@inheritDoc}
   *
   * Reads the broadcast sets for start vertex id and all vertex ids.
   */
  @Override
  public void preSuperstep() {
    startIds =  (List) getBroadcastSet(startIdsBroadcastSet);
    vertexUniqueIds = (List) getBroadcastSet(vertexUniqueLongIdsBroadcastSet);
    visitedVerticesAggregator = getIterationAggregator(visitedVerticesAggregatorName);
    currentVisitedCount = visitedVerticesAggregator.getAggregate().getValue();
    if (currentVisitedCount >= verticesToVisit) {
      stopIteration = true;
    }
  }

  /**
   * {@inheritDoc}
   *
   * Initially starts the first walk or jump from the given start vertex with the visited counter
   * set to {@code 0L}. Afterwards starts a walk or jump from a vertex, if it received a message
   * from another vertex, containing the visited counter.
   *
   * @param vertex The vertex this computation in a superstep is running for.
   * @param messages Iterator over all incoming messages
   */
  @Override
  public void compute(Vertex<Long, VCIVertexValue> vertex, MessageIterator<Long> messages) {

    VCIVertexValue updatedVertexValue = null;
    if (startIds.contains(vertex.getId()) && !vertex.getValue().isVisited()) {
      updatedVertexValue = walkOrJump(vertex, null);
    } else if (messages.hasNext()) {
      updatedVertexValue = walkOrJump(vertex, messages);
    }
    if (updatedVertexValue != null) {
      setNewVertexValue(updatedVertexValue);
    }
  }

  /**
   * Sets the visited value of a vertex to {@code true}, if this vertex was not yet visited.
   * Then starts a walk to a random neighbor from this vertex or a jump to a random vertex in the
   * graph by sending a message to this neighbor or vertex, passing the visited counter.
   * Neighbors for a walk are chosen uniformly at random out of all not yet visited neighbors.
   * Vertices for a jump are chosen uniformly at random out of all vertices from the graph, with
   * their ids provided {@link #vertexUniqueIds}.
   *
   * @param vertex The vertex this computation in a superstep is running for.
   * @param messages Messages passed to a vertex if this vertex is the target of a walk or jump.
   * @return The updated vertex value, or {@code null} if no updates were made
   */
  private VCIVertexValue walkOrJump(Vertex<Long, VCIVertexValue> vertex,
    MessageIterator<Long> messages) {

    VCIVertexValue vertexValue = vertex.getValue();

    // TODO: for each message (up to k messages possible)

    if (!stopIteration) {
      if (!vertexValue.isVisited()) {
        visitedVerticesAggregator.aggregate(1L);
        vertexValue.setVisited();
      }
      if (jumpProbability <= random.nextDouble()) {
        List<Tuple2<Long, Long>> unvisitedNeighborWithEdgeId = new ArrayList<>();
        for (Edge<Long, Long> edge : getEdges()) {
          if (!vertexValue.getVisitedOutEdges().contains(edge.getValue())) {
            unvisitedNeighborWithEdgeId.add(Tuple2.of(edge.getTarget(), edge.getValue()));
          }
        }
        if (!unvisitedNeighborWithEdgeId.isEmpty()) {
          if ((currentVisitedCount + 1L) < verticesToVisit) {
            int randomIndex = random.nextInt(unvisitedNeighborWithEdgeId.size());
            Long randomNeighborId = unvisitedNeighborWithEdgeId.get(randomIndex).f0;
            vertexValue.addVisitedOutEdge(unvisitedNeighborWithEdgeId.get(randomIndex).f1);
            sendMessageTo(randomNeighborId, vertex.getId());
          }
          return vertexValue;
        }
      }
      int randomIndex = random.nextInt(vertexUniqueIds.size());
      Long randomVertexId = (Long) vertexUniqueIds.get(randomIndex);
      sendMessageTo(randomVertexId, vertex.getId());
      return vertexValue;
    }
    return null;
  }
}
