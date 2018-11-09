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

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.gelly.randomjump.RandomJumpGellyVCI;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * <pre>
 * Compute function for {@link RandomJumpGellyVCI}.
 * Vertex values are of type {@link VCIVertexValue}, with
 *  - f0: {@code Boolean} set to {@code true} if the vertex was visited, {@code false} otherwise
 *  - f1: {@code List<GradoopId>} containing all ids from already visited neighbors over out edges
 *
 * A message from one vertex to another is considered a walk resp. a jump to this other vertex.
 * The message is of type {@code Long}, as a counter to keep track of the number of all visited
 * vertices in the graph.
 * </pre>
 */
public class VCIComputeFunction extends
  ComputeFunction<GradoopId, VCIVertexValue, NullValue, Long> {

  /**
   * Probability for jumping to a random vertex instead of walking to a random neighbor
   */
  private final double jumpProbability;

  /**
   * Number of vertices to visit via walk or jump
   */
  private final long verticesToVisit;

  /**
   * Name of the broadcast set containing the Id of the source vertex to start the RandomJump
   */
  private final String startIdBroadcastSet;

  /**
   * Id for the starting vertex
   */
  private GradoopId startId;

  /**
   * Name of the broadcast set containing the graphs vertex ids
   */
  private final String vertexIdsBroadcastSet;

  /**
   * List containing the graphs vertex ids
   */
  private List vertexIds;

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
   * @param startIdBroadcastSet Name of the broadcast set containing the Id of the vertex to start
   *                            the RandomJump
   * @param vertexIdsBroadcastSet Name of the broadcast set containing the ids of all vertices
   */
  public VCIComputeFunction(double jumpProbability, long verticesToVisit,
    String startIdBroadcastSet, String vertexIdsBroadcastSet) {
    this.jumpProbability = jumpProbability;
    this.verticesToVisit = verticesToVisit;
    this.startIdBroadcastSet = startIdBroadcastSet;
    this.vertexIdsBroadcastSet = vertexIdsBroadcastSet;
    this.random = new Random();
  }

  /**
   * {@inheritDoc}
   *
   * Reads the broadcast sets for start vertex id and all vertex ids.
   */
  @Override
  public void preSuperstep() {
    startId = (GradoopId) ((List) getBroadcastSet(startIdBroadcastSet)).get(0);
    vertexIds = (List) getBroadcastSet(vertexIdsBroadcastSet);
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
  public void compute(Vertex<GradoopId, VCIVertexValue> vertex, MessageIterator<Long> messages) {

    VCIVertexValue updatedVertexValue = null;
    if (vertex.getId().equals(startId) && !vertex.getValue().isVisited()) {
      updatedVertexValue = walkOrJump(vertex, 0L);
    } else if (messages.hasNext()) {
      updatedVertexValue = walkOrJump(vertex, messages.next());
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
   * their ids provided {@link #vertexIds}.
   *
   * @param vertex The vertex this computation in a superstep is running for.
   * @param visitedCount Counter passed to a vertex if this vertex is the target of a walk or jump.
   * @return The updated vertex value, or {@code null} if no updates were made
   */
  private VCIVertexValue walkOrJump(Vertex<GradoopId, VCIVertexValue> vertex, Long visitedCount) {

    VCIVertexValue vertexValue = vertex.getValue();

    if (visitedCount < verticesToVisit) {
      if (!vertexValue.isVisited()) {
        visitedCount += 1L;
        vertexValue.setVisited();
      }
      if (jumpProbability < random.nextDouble()) {
        List<GradoopId> unvisitedNeighbors = new ArrayList<>();
        for (Edge<GradoopId, NullValue> edge : getEdges()) {
          if (!vertexValue.getVisitedNeighbors().contains(edge.getTarget())) {
            unvisitedNeighbors.add(edge.getTarget());
          }
        }
        if (!unvisitedNeighbors.isEmpty()) {
          if (visitedCount < verticesToVisit) {
            int randomNeighborIndex = random.nextInt(unvisitedNeighbors.size());
            GradoopId randomNeighborId = unvisitedNeighbors.get(randomNeighborIndex);
            vertexValue.addVisitedNeighbor(randomNeighborId);
            sendMessageTo(randomNeighborId, visitedCount);
          }
          return vertexValue;
        }
      }
      int randomVertexIndex = random.nextInt(vertexIds.size());
      GradoopId randomVertexId = (GradoopId) vertexIds.get(randomVertexIndex);
      sendMessageTo(randomVertexId, visitedCount);
      return vertexValue;
    }
    return null;
  }
}
