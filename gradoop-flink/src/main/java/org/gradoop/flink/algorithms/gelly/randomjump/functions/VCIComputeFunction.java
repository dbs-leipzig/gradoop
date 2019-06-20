/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.algorithms.gelly.randomjump.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.gradoop.flink.algorithms.gelly.randomjump.KRandomJumpGellyVCI;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Compute function for {@link KRandomJumpGellyVCI}. Vertex values are of type
 * {@link VCIVertexValue}, with fields:
 * <p><ul>
 * <li>f0: {@code Boolean} set to {@code true} if the vertex was visited, to {@code false}
 * otherwise
 * <li>f1: {@code List<Long>} containing all long indices from already visited outgoing edges
 * </ul></p>
 * A message of type {@code NullValue} from one vertex to another is a walk respective a jump to
 * this other vertex and therefor considered as one of {@link KRandomJumpGellyVCI#k} walkers.
 */
public class VCIComputeFunction extends ComputeFunction<Long, VCIVertexValue, Long, NullValue> {

  /**
   * Name of the broadcast set containing the indices for the k starting vertices.
   */
  public static final String START_INDICES_BROADCAST_SET = "startIndices";

  /**
   * Name of the broadcast set containing the graphs vertex indices.
   */
  public static final String VERTEX_INDICES_BROADCAST_SET = "vertexIndices";

  /**
   * Name for the LongSumAggregator used for counting the visited vertices.
   */
  public static final String VISITED_VERTICES_AGGREGATOR_NAME = "visitedVerticesAggregator";

  /**
   * Probability for jumping to a random vertex instead of walking to a random neighbor.
   */
  private final double jumpProbability;

  /**
   * Number of vertices to visit at least.
   */
  private final long verticesToVisit;

  /**
   * List with the indices for the k starting vertices.
   */
  private List<Long> startIndices;

  /**
   * List containing the graphs vertex indices.
   */
  private List<Long> vertexIndices;

  /**
   * The LongSumAggregator used for counting the visited vertices.
   */
  private LongSumAggregator visitedVerticesAggregator;

  /**
   * Keeping track of the currently visited vertices at the beginning at each superstep.
   */
  private long currentVisitedCount;

  /**
   * Creates an instance of VCIComputeFunction.
   *
   * @param jumpProbability Probability for jumping to random vertex instead of walking to random
   *                        neighbor.
   * @param verticesToVisit Number of vertices to visit via walk or jump.
   */
  public VCIComputeFunction(double jumpProbability, long verticesToVisit) {
    this.jumpProbability = jumpProbability;
    this.verticesToVisit = verticesToVisit;
    this.visitedVerticesAggregator = new LongSumAggregator();
    this.currentVisitedCount = 0L;
  }

  /**
   * {@inheritDoc}
   *
   * Reads the broadcast sets for the starting vertices and the graph vertices. Retrieves the
   * aggregator for visited vertices and accumulates the visited vertices from the previous
   * superstep.
   */
  @Override
  public void preSuperstep() {
    startIndices = (List<Long>) this.<Long>getBroadcastSet(START_INDICES_BROADCAST_SET);
    vertexIndices = (List<Long>) this.<Long>getBroadcastSet(VERTEX_INDICES_BROADCAST_SET);
    visitedVerticesAggregator = getIterationAggregator(VISITED_VERTICES_AGGREGATOR_NAME);
    LongValue previousAggregate = getPreviousIterationAggregate(VISITED_VERTICES_AGGREGATOR_NAME);
    if (previousAggregate != null) {
      currentVisitedCount += previousAggregate.getValue();
    }
  }

  /**
   * {@inheritDoc}
   *
   * Initially starts the first walk or jump from the given start vertices. Afterwards starts a
   * walk or jump from a vertex, if it received messages from other vertices. Stops the
   * computation and therefor the iteration if the number of currently visited vertices exceeds
   * the number of vertices to visit.
   *
   * @param vertex The vertex this computation in a superstep is running for.
   * @param messages Iterator over all incoming messages
   */
  @Override
  public void compute(Vertex<Long, VCIVertexValue> vertex, MessageIterator<NullValue> messages) {
    if (currentVisitedCount < verticesToVisit) {
      List<Edge<Long, Long>> edgesList = Lists.newArrayList(getEdges());
      Tuple2<VCIVertexValue, Boolean> valueWithHasChanged = Tuple2.of(vertex.getValue(), false);
      if (startIndices.contains(vertex.getId()) && !valueWithHasChanged.f0.isVisited()) {
        valueWithHasChanged = walkToRandomNeighbor(valueWithHasChanged, edgesList);
      } else if (messages.hasNext()) {
        for (NullValue msg : messages) {
          valueWithHasChanged = walkToRandomNeighbor(valueWithHasChanged, edgesList);
        }
      }
      if (valueWithHasChanged.f1) {
        setNewVertexValue(valueWithHasChanged.f0);
      }
    }
  }

  /**
   * Performs a walk to a random neighbor by sending a message to a target from an unvisited
   * outgoing edge. Sets the vertex value as visited if necessary and updates the visited edge ids.
   * Sets a boolean flag, if the vertex value has changed.
   * Alternatively performs a jump to a random vertex with a probability given in
   * {@link #jumpProbability} or if there are no unvisited outgoing edges.
   * Returns the vertex value with the boolean flag as {@code Tuple2} eventually.
   *
   * @param valueWithHasChanged {@code Tuple2} containing the vertex value and a boolean flag
   *                            determining if the value has changed
   * @param edgesList List of all outgoing edge for the vertex
   * @return {@code Tuple2} containing the vertex value and a boolean flag determining if the
   *         value has changed
   */
  private Tuple2<VCIVertexValue, Boolean> walkToRandomNeighbor(
    Tuple2<VCIVertexValue, Boolean> valueWithHasChanged, List<Edge<Long, Long>> edgesList) {
    if (!valueWithHasChanged.f0.isVisited()) {
      visitedVerticesAggregator.aggregate(1L);
      valueWithHasChanged.f0.setVisited();
      valueWithHasChanged.f1 = true;
    }
    if ((jumpProbability == 0d) || (jumpProbability < ThreadLocalRandom.current().nextDouble())) {
      List<Tuple2<Long, Long>> unvisitedNeighborWithEdgeId = new ArrayList<>();
      for (Edge<Long, Long> edge : edgesList) {
        if (!valueWithHasChanged.f0.getVisitedOutEdges().contains(edge.getValue())) {
          unvisitedNeighborWithEdgeId.add(Tuple2.of(edge.getTarget(), edge.getValue()));
        }
      }
      if (!unvisitedNeighborWithEdgeId.isEmpty()) {
        int randomIndex = ThreadLocalRandom.current().nextInt(unvisitedNeighborWithEdgeId.size());
        Long randomNeighborIndex = unvisitedNeighborWithEdgeId.get(randomIndex).f0;
        valueWithHasChanged.f0.addVisitedOutEdge(unvisitedNeighborWithEdgeId.get(randomIndex).f1);
        sendMessageTo(randomNeighborIndex, new NullValue());
        valueWithHasChanged.f1 = true;
      } else {
        jumpToRandomVertex();
      }
    } else {
      jumpToRandomVertex();
    }
    return valueWithHasChanged;
  }

  /**
   * Jumps to a random vertex in the graph by sending a message to this vertex.
   */
  private void jumpToRandomVertex() {
    int randomIndex = ThreadLocalRandom.current().nextInt(vertexIndices.size());
    Long randomVertexIndex = vertexIndices.get(randomIndex);
    sendMessageTo(randomVertexIndex, new NullValue());
  }
}
