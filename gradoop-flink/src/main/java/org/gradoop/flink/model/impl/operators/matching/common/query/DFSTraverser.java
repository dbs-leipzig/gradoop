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
  public QueryHandler getQueryHandler() {
    return queryHandler;
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
