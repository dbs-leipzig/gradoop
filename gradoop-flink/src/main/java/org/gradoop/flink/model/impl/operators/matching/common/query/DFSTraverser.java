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

package org.gradoop.flink.model.impl.operators.matching.common.query;

import com.google.common.collect.Sets;
import org.s1ck.gdl.model.Edge;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Set;

/**
 * Implementation of a {@link Traverser}, using a depth-first search.
 */
public class DFSTraverser implements RootedTraverser {

  /**
   * Query handler to access the query graph.
   */
  private QueryHandler queryHandler;

  @Override
  public TraversalCode traverse() {
    return traverse(queryHandler.getVertices().iterator().next().getId());
  }

  @Override
  public void setQueryHandler(QueryHandler queryHandler) {
    this.queryHandler = queryHandler;
  }

  @Override
  public TraversalCode traverse(long rootVertex) {
    Set<Long> vertexVisited = Sets.newHashSetWithExpectedSize(
      queryHandler.getVertexCount());
    Set<Long> edgeVisited = Sets.newHashSetWithExpectedSize(
      queryHandler.getEdgeCount());
    TraversalCode traversalCode = new TraversalCode();

    long current = rootVertex;

    vertexVisited.add(current);
    Deque<Edge> edgeStack = new ArrayDeque<>();
    edgeStack.addAll(queryHandler.getEdgesByVertexId(current));

    while (!edgeStack.isEmpty()) {
      Edge edge = edgeStack.removeLast();
      long via = edge.getId();

      if (!edgeVisited.contains(via)) {
        long source = edge.getSourceVertexId();
        long target = edge.getTargetVertexId();

        // backtrack?
        if (current != source && current != target) {
          current = vertexVisited.contains(source) ? source : target;
        }

        // do step
        boolean isOutgoing  = current == source;
        long from           = isOutgoing ? source : target;
        long to             = isOutgoing ? target : source;

        traversalCode.add(new Step(from, via, to, isOutgoing));

        // go deeper
        boolean visitedFrom = vertexVisited.contains(from);
        boolean visitedTo   = vertexVisited.contains(to);

        if (!visitedFrom || !visitedTo) {
          current = visitedFrom ? to : from;
          edgeStack.addAll(queryHandler.getEdgesByVertexId(current));
        }

        vertexVisited.add(current);
        edgeVisited.add(via);
      }
    }

    return traversalCode;
  }
}
