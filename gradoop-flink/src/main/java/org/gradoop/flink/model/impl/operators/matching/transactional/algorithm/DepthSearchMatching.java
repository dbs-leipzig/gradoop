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
package org.gradoop.flink.model.impl.operators.matching.transactional.algorithm;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.Embedding;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.transactional.tuples.GraphWithCandidates;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.Vertex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;


/**
 * This is an implementation of a very straight-forward depth-first pattern
 * matching algorithm.
 *
 * The depth-first search is implemented by popping an embedding from
 * a stack, growing it by one vertex and its corresponding edges, and adding
 * the resulting new edges to the top of the stack. If it is not possible to
 * apply a step to an existing embedding, the embedding is removed.
 * This is repeated until all embeddings are either completed
 * (all steps have been applied).
 */
public class DepthSearchMatching implements PatternMatchingAlgorithm {
  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;
  /**
   * vertex-id -> vertex candidates
   */
  private Map<GradoopId, boolean[]> vertexDict;
  /**
   * source-id -> edge-ids
   */
  private Map<GradoopId, Set<GradoopId>> sourceDict;
  /**
   * target-id -> edge-ids
   */
  private Map<GradoopId, Set<GradoopId>> targetDict;
  /**
   * edge id -> source-id, target-id, edge candidates
   */
  private Map<GradoopId, Tuple3<GradoopId, GradoopId, boolean[]>> edgeDict;
  /**
   * QueryHandler
   */
  private transient QueryHandler handler;

  /**
   * Constructor
   */
  public DepthSearchMatching() {
    this.vertexDict = new HashMap<>();
    this.sourceDict = new HashMap<>();
    this.targetDict = new HashMap<>();
    this.edgeDict = new HashMap<>();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<Embedding<GradoopId>> findEmbeddings(GraphWithCandidates graph,
    String query) {
    // construct a handler for the query
    handler = new QueryHandler(query);

    // build the plan that determines in which order vertices should added
    int[] plan = buildQueryPlan();

    // by using a stack (first in - last out) we implement a depth-first search
    Stack<Embedding<GradoopId>> embeddings = new Stack<>();

    // construct an empty embedding
    Embedding<GradoopId> firstEmbedding = new Embedding<>();
    firstEmbedding.setVertexMapping(new GradoopId[handler.getVertexCount()]);
    firstEmbedding.setEdgeMapping(new GradoopId[handler.getEdgeCount()]);
    embeddings.push(firstEmbedding);

    // initialize the vertex, edge, source and target maps
    initializeMaps(graph);

    List<Embedding<GradoopId>> results = new ArrayList<>();

    // grow the embeddings until there are none left
    while (!embeddings.isEmpty()) {
      Embedding<GradoopId> embedding = embeddings.pop();

      // find the first step in the plan that has not been applied yet
      int nextStep = -1;
      for (int step : plan) {
        if (embedding.getVertexMapping()[step] == null) {
          nextStep = step;
          break;
        }
      }

      // if all steps have been applied, add the embedding to the results
      if (nextStep == -1) {
        results.add(embedding);
        continue;
      }

      // compute new embeddings by applying the step on the current embedding
      List<Embedding<GradoopId>> newEmbeddings = executeStep(embedding, nextStep);

      // add the new embeddings on top of the stack
      embeddings.addAll(newEmbeddings);
    }

    // reset the maps for the next graph
    resetMaps();

    return results;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Boolean hasEmbedding(GraphWithCandidates graph, String query) {

    // construct a handler for the query
    handler = new QueryHandler(query);

    // build the plan that determines in which order vertices should added
    int[] plan = buildQueryPlan();

    // by using a stack (first in - last out) we implement a depth-first search
    Stack<Embedding<GradoopId>> embeddings = new Stack<>();

    // construct an empty embedding
    Embedding<GradoopId> firstEmbedding = new Embedding<>();
    firstEmbedding.setVertexMapping(new GradoopId[handler.getVertexCount()]);
    firstEmbedding.setEdgeMapping(new GradoopId[handler.getEdgeCount()]);
    embeddings.push(firstEmbedding);

    // initialize the vertex, edge, source and target maps
    initializeMaps(graph);

    // grow the embeddings until there are none left
    while (!embeddings.isEmpty()) {
      Embedding<GradoopId> embedding = embeddings.pop();

      // find the first step in the plan that has not been applied yet
      int nextStep = -1;
      for (int step : plan) {
        if (embedding.getVertexMapping()[step] == null) {
          nextStep = step;
          break;
        }
      }

      // if there is a complete embedding, return true
      if (nextStep == -1) {
        resetMaps();
        return true;
      }

      // compute new embeddings by applying the step on the current embedding
      List<Embedding<GradoopId>> newEmbeddings = executeStep(embedding, nextStep);

      // add the new embeddings on top of the stack
      embeddings.addAll(newEmbeddings);
    }

    // reset the maps for the next graph
    resetMaps();

    // if no complete embedding could be constructed, return false
    return false;
  }

  /**
   * Reset the maps for the next graph.
   */
  private void resetMaps() {
    vertexDict.clear();
    edgeDict.clear();
    sourceDict.clear();
    targetDict.clear();
  }

  /**
   * Execute a step. A step corresponds to the position of the next vertex in
   * the vertex mappings of the embedding that shall be matched.
   *
   * @param embedding the embedding that has been constructed so far
   * @param step      number of the next vertex to be matched
   * @return list of newly constructed embeddings
   */
  private List<Embedding<GradoopId>> executeStep(Embedding<GradoopId> embedding, int step) {

    // map containing the found matches for all pattern edges
    // this is necessary to be able to construct all valid permutations of edges
    Map<Long, Set<GradoopId>> edgeMatches = new HashMap<>();

    List<Embedding<GradoopId>> results = new ArrayList<>();

    // get the possible vertex matches
    for (GradoopId id : getCandidates(step)) {

      // flag to recognize failed vertex matchings
      boolean failed = false;

      // if the vertex is already in the embedding, skip it
      if (Arrays.asList(embedding.getVertexMapping()).contains(id)) {
        continue;
      }

      // get all outgoing edges of the next step vertex
      Collection<Edge> edges = handler.getEdgesBySourceVertexId((long) step);
      List<Edge> patternEdges =
        edges != null ? new ArrayList<>(edges) : new ArrayList<>();

      // only keep those edges that have at least one vertex already in the
      // embedding, or are direct loops
      filterPatternEdges(patternEdges, embedding);

      // find the matches for each pattern edge
      for (Edge patternEdge : patternEdges) {
        edgeMatches.put(patternEdge.getId(), new HashSet<>());

        // find all edge candidates
        Set<GradoopId> edgeCandidateIds =
          sourceDict.get(id) != null ? sourceDict.get(id) : new HashSet<>();

        // for each candidate, check if it matches the pattern edge
        for (GradoopId edgeCandidateId : edgeCandidateIds) {

          Tuple3<GradoopId, GradoopId, boolean[]> edgeCandidate =
            edgeDict.get(edgeCandidateId);

          // get the target vertex of the edge
          GradoopId target = embedding.getVertexMapping()[
            Math.toIntExact(patternEdge.getTargetVertexId())];

          // if the pattern edge and the edge candidate are both loops
          // and the candidate matches, add it to the embedding
          if (isLoop(patternEdge, edgeCandidate)) {
            edgeMatches.get(patternEdge.getId()).add(edgeCandidateId);

            // else, if the candidate matches the pattern edge,
            // add it to the embedding
          } else if (matchOutgoingEdge(patternEdge, edgeCandidateId, target)) {
            edgeMatches.get(patternEdge.getId()).add(edgeCandidateId);
          }
        }

        // if there was no matching edge for a pattern edge, the candidate can be discarded
        if (edgeMatches.get(patternEdge.getId()).isEmpty()) {
          failed = true;
          break;
        }
      }

      if (failed) {
        continue;
      }

      // get all incoming edges of the next step vertex
      edges = handler.getEdgesByTargetVertexId((long) step);
      patternEdges = edges != null ? new ArrayList<>(edges) : new ArrayList<>();

      // only keep those edges that have at least one vertex already in the
      // embedding, or are direct loops
      filterPatternEdges(patternEdges, embedding);

      // find the matches for each pattern edge
      for (Edge patternEdge : patternEdges) {
        edgeMatches.put(patternEdge.getId(), new HashSet<>());

        // find all edge candidates
        Set<GradoopId> edgeCandidateIds =
          targetDict.get(id) != null ? targetDict.get(id) : new HashSet<>();

        // for each candidate, check if it matches the pattern edge
        for (GradoopId edgeCandidateId : edgeCandidateIds) {

          Tuple3<GradoopId, GradoopId, boolean[]> edgeCandidate =
            edgeDict.get(edgeCandidateId);

          // get the target vertex of the edge
          GradoopId source = embedding.getVertexMapping()[
            Math.toIntExact(patternEdge.getSourceVertexId())];

          // if the pattern edge and the edge candidate are both loops
          // and the candidate matches, add it to the embedding
          if (isLoop(patternEdge, edgeCandidate)) {
            edgeMatches.get(patternEdge.getId()).add(edgeCandidateId);

            // else, if the candidate matches the pattern edge,
            // add it to the embedding
          } else if (matchIncomingEdge(patternEdge, edgeCandidateId, source)) {
            edgeMatches.get(patternEdge.getId()).add(edgeCandidateId);
          }
        }

        // if there was no matching edge for a pattern edge, the candidate
        // can be discarded
        if (edgeMatches.get(patternEdge.getId()).isEmpty()) {
          failed = true;
          break;
        }
      }

      if (failed) {
        continue;
      }
      // add all grown embeddings to the results
      results.addAll(buildNewEmbeddings(embedding, step, id, edgeMatches));
    }
    return results;
  }

  /**
   * If both the pattern edge and the edge candidate are loops and the candidate
   * matches the pattern, return true.
   *
   * @param patternEdge pattern edge
   * @param edgeCandidate edge candidate
   * @return true iff both are loops and the candidate matches the pattern edge
   */
  private boolean isLoop(Edge patternEdge,
    Tuple3<GradoopId, GradoopId, boolean[]> edgeCandidate) {
    if (patternEdge.getSourceVertexId()
      .equals(patternEdge.getTargetVertexId())) {
      if (edgeCandidate.f0.equals(edgeCandidate.f1)) {
        if (edgeCandidate.f2[(int) patternEdge.getId()]) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Remove all edges that are neither loops nor have source or target vertex
   * that is in the embedding.
   *
   * @param patternEdges edges to be filtered
   * @param embedding embedding with possible source and target vertices
   * @return filtered edge list
   */
  private List<Edge> filterPatternEdges(
    List<Edge> patternEdges,
    Embedding<GradoopId> embedding) {
    for (int i = 0; i < patternEdges.size(); i++) {
      long sourceId = patternEdges.get(i).getSourceVertexId();
      long targetId = patternEdges.get(i).getTargetVertexId();
      GradoopId[] vertexMapping = embedding.getVertexMapping();
      if ((vertexMapping[(int) sourceId] == null) &&
        (vertexMapping[(int) targetId] == null)) {
        if (sourceId != targetId) {
          patternEdges.remove(i);
          i--;
        }
      }
    }
    return patternEdges;
  }

  /**
   * Extend the given embedding by found matches
   *
   * @param startEmbedding embedding that is to be extended
   * @param step next step
   * @param vertexMatch vertex chosen for next step
   * @param edgeMatches found match
   * @return extended embedding
   */
  private List<Embedding<GradoopId>> buildNewEmbeddings(
    Embedding<GradoopId> startEmbedding,
    int step,
    GradoopId vertexMatch,
    Map<Long, Set<GradoopId>> edgeMatches) {

    // begin with the start embedding
    List<Embedding<GradoopId>> result = new ArrayList<>();
    result.add(startEmbedding);

    // list of embeddings in next growing step
    List<Embedding<GradoopId>> temporaryEmbeddings;

    // get edge matches
    List<Map.Entry<Long, Set<GradoopId>>> entries =
      new ArrayList<>(edgeMatches.entrySet());

    // if no edge matches have been found, the result is an embedding with only
    // one vertex
    if (entries.isEmpty()) {
      Embedding<GradoopId> newEmbedding = copyEmbedding(startEmbedding);
      newEmbedding.getVertexMapping()[step] = vertexMatch;
      result.clear();
      result.add(newEmbedding);
    }

    // for each edge with matches
    for (Map.Entry<Long, Set<GradoopId>> entry : entries) {
      temporaryEmbeddings = new ArrayList<>();
      // for each embedding in the result so far
      for (Embedding<GradoopId> embedding : result) {
        // for each edge match
        for (GradoopId id : entry.getValue()) {
          // check if the edge match already occurs in the embedding
          boolean contains = false;
          for (GradoopId edgeId : embedding.getEdgeMapping()) {
            if (edgeId == null) {
              continue;
            }
            if (edgeId.equals(id)) {
              contains = true;
              break;
            }
          }
          // if yes, skip it
          if (contains) {
            continue;
          }
          // else, copy the embedding, add the edge match and add it to the
          // results in the next iteration
          Embedding<GradoopId> newEmbedding = copyEmbedding(embedding);
          newEmbedding.getVertexMapping()[step] = vertexMatch;
          newEmbedding.getEdgeMapping()[Math.toIntExact(entry.getKey())] = id;
          temporaryEmbeddings.add(newEmbedding);
        }
      }
      result = new ArrayList<>(temporaryEmbeddings);
    }
    return result;
  }

  /**
   * Creates a copy of an existing embedding.
   *
   * @param existing existing embedding
   * @return copy of the existing embedding
   */
  private Embedding<GradoopId> copyEmbedding(Embedding<GradoopId> existing) {
    Embedding<GradoopId> newEmbedding = new Embedding<>();

    GradoopId[] vertexCopy = new GradoopId[existing.getVertexMapping().length];
    System.arraycopy(existing.getVertexMapping(), 0, vertexCopy, 0,
      existing.getVertexMapping().length);

    GradoopId[] edgeCopy = new GradoopId[existing.getEdgeMapping().length];
    System.arraycopy(existing.getEdgeMapping(), 0, edgeCopy, 0,
      existing.getEdgeMapping().length);

    newEmbedding.setVertexMapping(vertexCopy);
    newEmbedding.setEdgeMapping(edgeCopy);
    return newEmbedding;
  }

  /**
   * Get all vertices that match the pattern of a step.
   *
   * @param step next step in the plan
   * @return list containing the ids of the matching vertices
   */
  public List<GradoopId> getCandidates(int step) {
    List<GradoopId> possibilities = new ArrayList<>(vertexDict.keySet());
    for (int i = 0; i < possibilities.size(); i++) {
      GradoopId vertexId = possibilities.get(i);
      boolean[] candidates = vertexDict.get(vertexId);
      if (!candidates[step]) {
        possibilities.remove(i);
        --i;
      }
    }
    return possibilities;
  }

  /**
   * Returns true, if the edge candidate matches the outgoing pattern edge.
   *
   * @param patternEdge outgoing pattern edge
   * @param edgeCandidate id of the edge candidate
   * @param target target id of the edge candidate
   * @return true, iff edge matches pattern edge
   */
  private boolean matchOutgoingEdge(Edge patternEdge, GradoopId edgeCandidate,
    GradoopId target) {
    Tuple3<GradoopId, GradoopId, boolean[]> edge = edgeDict.get(edgeCandidate);
    if (edge.f2[(int) patternEdge.getId()]) {
      GradoopId possibleVertex = edge.f1;
      if (possibleVertex.equals(target)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns true, if the edge candidate matches the incoming pattern edge.
   *
   * @param patternEdge incoming pattern edge
   * @param edgeCandidate id of the edge candidate
   * @param source source id of the edge candidate
   * @return true, iff edge matches pattern edge
   */
  private boolean matchIncomingEdge(Edge patternEdge, GradoopId edgeCandidate,
    GradoopId source) {
    Tuple3<GradoopId, GradoopId, boolean[]> edge = edgeDict.get(edgeCandidate);
    if (edge.f2[(int) patternEdge.getId()]) {
      GradoopId possibleVertex = edge.f0;
      if (possibleVertex.equals(source)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Build HashMaps for faster access.
   *
   * @param graph graphs with candidate sets
   */
  private void initializeMaps(GraphWithCandidates graph) {
    for (IdWithCandidates<GradoopId> vertex : graph.getVertexCandidates()) {
      vertexDict.put(vertex.getId(), vertex.getCandidates());
    }

    for (TripleWithCandidates<GradoopId> edge : graph.getEdgeCandidates()) {
      edgeDict.put(edge.getEdgeId(),
        new Tuple3<>(edge.getSourceId(), edge.getTargetId(),
          edge.getCandidates()));
    }

    for (TripleWithCandidates<GradoopId> edge : graph.getEdgeCandidates()) {
      if (!sourceDict.containsKey(edge.getSourceId())) {
        sourceDict.put(edge.getSourceId(), new HashSet<>());
      }
      sourceDict.get(edge.getSourceId()).add(edge.getEdgeId());
      if (!targetDict.containsKey(edge.getTargetId())) {
        targetDict.put(edge.getTargetId(), new HashSet<>());
      }
      targetDict.get(edge.getTargetId()).add(edge.getEdgeId());
    }
  }

  /**
   * Method to create query plan, determining how patterns are grown.
   *
   * @return plan for walk through
   */
  private int[] buildQueryPlan() {
    int[] queryPlan = new int[handler.getVertices().size()];
    int step = 0;
    Set<Long> alreadyVisited = new HashSet<>();
    Stack<Vertex> stack = new Stack<>();
    stack.push(handler.getVertexById(0L));
    alreadyVisited.add(0L);
    while (!stack.isEmpty()) {
      Vertex current = stack.pop();
      Collection<Vertex> neighbors = handler.getNeighbors(current.getId());
      queryPlan[step] = (int) current.getId();
      step++;
      neighbors.stream()
        .filter(neighbor -> !alreadyVisited.contains(neighbor.getId()))
        .forEach(neighbor -> {
            alreadyVisited.add(neighbor.getId());
            stack.push(neighbor);
          }
        );
    }
    return queryPlan;
  }
}
