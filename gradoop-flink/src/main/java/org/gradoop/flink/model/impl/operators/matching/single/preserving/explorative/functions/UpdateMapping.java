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
package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.functions;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.query.Step;
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.Embedding;

import java.util.HashSet;
import java.util.Set;

/**
 * Base class for updating vertex and edge mappings in an {@link Embedding}.
 *
 * @param <K> key type
 */
abstract class UpdateMapping<K> extends AbstractRichFunction {
  /**
   * Traversal code representing the graph query
   */
  private final TraversalCode traversalCode;
  /**
   * Match strategy for morphism checks
   */
  private final MatchStrategy matchStrategy;
  /**
   * Total number of steps in the traversal
   */
  private final int stepCount;
  /**
   * Id of the current step
   */
  private int currentStepId;
  /**
   * Flag to signal if there is a preceding step in the traversal
   */
  private boolean hasMoreSteps;
  /**
   * Represents a list of all edge candidates visited in previous traversal steps
   */
  private int[] previousEdgeCandidates;
  /**
   * Represents a set of all vertex candidates visited in previous traversal steps
   */
  private int[] previousVertexCandidates;

  /**
   * Constructor
   *
   * @param traversalCode traversal code representing the graph query
   * @param matchStrategy morphism strategy
   */
  UpdateMapping(TraversalCode traversalCode, MatchStrategy matchStrategy) {
    this.traversalCode = traversalCode;
    this.matchStrategy = matchStrategy;
    this.stepCount = traversalCode.getSteps().size();
  }

  /**
   * Check if the given id has been visited before.
   *
   * @param mapping current mapping
   * @param id      id to check
   * @param fields  fields to check
   * @return true, if id was visited before
   */
  private boolean seenBefore(K[] mapping, K id, int[] fields) {
    boolean result = false;

    for (int field : fields) {
      if (mapping[field].equals(id)) {
        result = true;
        break;
      }
    }
    return result;
  }

  /**
   * If the given {@link MatchStrategy} requires it, this method initializes the collections
   * of previously visited edges and vertices according to the traversal code.
   */
  void initializeVisited() {
    if (getMatchStrategy() == MatchStrategy.ISOMORPHISM) {
      // get previous edge candidates
      previousEdgeCandidates = new int[currentStepId];
      for (int i = 0; i < currentStepId; i++) {
        previousEdgeCandidates[i] = (int) getTraversalCode().getStep(i).getVia();
      }

      // get previous vertex candidates (limited by two times the number steps (edges))
      Set<Integer> visitedVertices = new HashSet<>(stepCount * 2);
      for (int i = 0; i < currentStepId; i++) {
        Step s = getTraversalCode().getStep(i);
        visitedVertices.add((int) s.getFrom());
        visitedVertices.add((int) s.getTo());
      }
      // add from field of current step
      visitedVertices.add((int) getCurrentStep().getFrom());
      // initialize array from unique vertex ids
      previousVertexCandidates = new int[visitedVertices.size()];
      int i = 0;
      for (Integer visitedVertex : visitedVertices) {
        previousVertexCandidates[i++] = visitedVertex;
      }
    }
  }

  TraversalCode getTraversalCode() {
    return traversalCode;
  }

  MatchStrategy getMatchStrategy() {
    return matchStrategy;
  }

  /**
   * Sets the current step to the specified value. The current step needs to be provided
   * by the inheriting classes.
   *
   * @param currentStepId current step
   */
  void setCurrentStepId(int currentStepId) {
    this.currentStepId = currentStepId;
    this.hasMoreSteps = currentStepId < stepCount - 1;
  }

  Step getCurrentStep() {
    return this.traversalCode.getStep(currentStepId);
  }

  /**
   * Check if there are more traversal steps left.
   *
   * @return true, if there are more steps
   */
  boolean hasMoreSteps() {
    return hasMoreSteps;
  }

  /**
   * Returns the from-candidate of the next traversal step or {@code -1} if it is the last
   * step.
   *
   * @return from candidate of next step or -1 if there is none
   */
  int getNextFrom() {
    return hasMoreSteps() ? (int) getTraversalCode()
      .getStep(currentStepId + 1)
      .getFrom() : -1;
  }

  /**
   * Checks if the given vertex is a valid candidate for the specified mapping index.
   *
   * @param vertexId vertex id to check for validity
   * @param vertexMapping current vertex mapping
   * @param candidateIndex position in the mapping to check validity for
   * @return true, iff the specified vertex is a valid candidate for the specified candidateIndex
   */
  boolean isValidVertex(K vertexId, K[] vertexMapping, int candidateIndex) {
    boolean isMapped = vertexMapping[candidateIndex] != null;
    boolean seen = matchStrategy == MatchStrategy.ISOMORPHISM &&
      seenBefore(vertexMapping, vertexId, previousVertexCandidates);
    return (!isMapped && !seen) || (isMapped && vertexMapping[candidateIndex].equals(vertexId));
  }

  /**
   * Checks of the given edge is a valid candidate for the specified mapping index.
   *
   * @param edgeId edge id to check for validity
   * @param edgeMapping current edge mapping
   * @param candidateIndex position in the mapping to check validity for
   * @return true, iff the specified edge is a valid candidate for the specified candidateIndex
   */
  boolean isValidEdge(K edgeId, K[] edgeMapping, int candidateIndex) {
    boolean isMapped = edgeMapping[candidateIndex] != null;
    boolean seen = getMatchStrategy() == MatchStrategy.ISOMORPHISM &&
      seenBefore(edgeMapping, edgeId, previousEdgeCandidates);
    return !isMapped && !seen;
  }
}
