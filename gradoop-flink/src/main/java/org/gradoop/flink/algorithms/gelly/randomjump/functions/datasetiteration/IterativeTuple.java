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
package org.gradoop.flink.algorithms.gelly.randomjump.functions.datasetiteration;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.List;
import java.util.stream.Collectors;

/**
 * <pre>
 * Data type used for the iterative dataset. It is derived from {@link Tuple4} and provides
 * encapsulating methods for the tuple values. Its values are:
 *  - {@code Long} - Flag determining if a tuple is active ({@code 1L}), or inactive ({@code 0L})
 *                   in an iteration step
 *  - {@code GradoopId} - Id for the source vertex this tuple was created for
 *  - {@code Long} - Flag determining, if the source vertex was visited ({@code true}),
 *                   or not ({@code false})
 *  - {@code List} - containing all outgoing edges for the source vertex, with edges of type
 *                   {@link EdgeType}
 * </pre>
 */
public class IterativeTuple extends
  Tuple4<Long, GradoopId, Long, List<IterativeTuple.EdgeType>> {

  /**
   * Creates an empty instance for IterativeTuple.
   */
  public IterativeTuple() { }

  /**
   * Creates an instance for IterativeTuple with the given initial values.
   *
   * @param active Flag determining if a tuple is active ({@code 1L}), or inactive ({@code 0L})
   *               in an iteration step
   * @param sourceId Id for the source vertex this tuple was created for
   * @param visited Flag determining if the source vertex was visited ({@code 1L}),
   *                or not ({@code 0L})
   * @param edges List of outgoing edges for the source vertex, with edges of type {@link EdgeType}
   */
  public IterativeTuple(Long active, GradoopId sourceId, Long visited,
    List<EdgeType> edges) {
    super(active, sourceId, visited, edges);
  }

  /**
   * Gets the id for the tuples source vertex
   *
   * @return source vertex id
   */
  public GradoopId getSourceId() {
    return this.f1;
  }

  /**
   * Sets the tuple as active
   */
  public void setTupleActive() {
    this.f0 = 1L;
  }

  /**
   * Sets the tuple as inactive
   */
  public void setTupleInactive() {
    this.f0 = 0L;
  }

  /**
   * Checks if the tuple is active in an iteration step
   *
   * @return {@code true} if active, {@code false} otherwise
   */
  public boolean isTupleActive() {
    return this.f0 == 1L;
  }

  /**
   * Sets the source vertex as visited
   */
  public void setSourceVisited() {
    this.f2 = 1L;
  }

  /**
   * Checks if the source vertex was visited
   *
   * @return {@code true} if visited, {@code false} otherwise
   */
  public boolean isSourceVisited() {
    return this.f2 == 1L;
  }

  /**
   * Gets the value for the number of all visited vertices. Used after a sum aggregation over all
   * visited flags in the dataset.
   *
   * @return Number of all visited vertices
   */
  public long getVisitedCount() {
    return this.f2;
  }

  /**
   * Gets all outgoing edges for the tuples source vertex
   *
   * @return All outgoing edges
   */
  public List<EdgeType> getEdges() {
    return this.f3;
  }

  /**
   * Sets the outgoing edges for the tuples source vertex
   *
   * @param edges Outgoing edges
   */
  public void setEdges(List<EdgeType> edges) {
    this.f3 = edges;
  }

  /**
   * Gets all visited edges for the tuples source vertex, determined by a flag
   * set in {@link EdgeType}
   *
   * @return All visited edges
   */
  public List<EdgeType> getVisitedEdges() {
    return this.getEdges().stream().filter(edge -> edge.f0 == 1L).collect(Collectors.toList());
  }

  /**
   * Gets all unvisited edges for the tuples source vertex, determined by a flag
   * set in {@link EdgeType}
   *
   * @return All unvisited edges
   */
  public List<EdgeType> getUnvisitedEdges() {
    return this.getEdges().stream().filter(edge -> edge.f0 == 0L).collect(Collectors.toList());
  }

  /**
   * Data type for the outgoing edges for the tuples source vertex.
   */
  public static class EdgeType extends Tuple3<Long, GradoopId, Double> {

    /**
     * Creates an empty instance of EdgeType
     */
    public EdgeType() { }

    /**
     * Creates an instance of EdgeType with the given initial values.
     *
     * @param visited Flag determining if an edge was visited ({@code 1L}), or not ({@code 0L})
     * @param targetId Id for the edges target
     * @param jumpProbability Value for the jump probability, stored here for reasons regarding
     *                        the algorithm design and simplifying the access to this value
     */
    public EdgeType(Long visited, GradoopId targetId, Double jumpProbability) {
      super(visited, targetId, jumpProbability);
    }

    /**
     * Sets the edge as visited
     */
    public void setEdgeVisited() {
      this.f0 = 1L;
    }

    /**
     * Gets the edges target id
     *
     * @return Target id
     */
    public GradoopId getTargetId() {
      return this.f1;
    }

    /**
     * Gets the stored jump probability value
     *
     * @return jump probability value
     */
    public Double getJumpProbability() {
      return this.f2;
    }
  }
}
