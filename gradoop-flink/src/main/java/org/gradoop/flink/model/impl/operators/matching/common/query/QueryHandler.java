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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.gradoop.common.util.GradoopConstants;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryPredicate;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.GraphElement;
import org.s1ck.gdl.model.Vertex;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Wraps a {@link GDLHandler} and adds functionality needed for query
 * processing during graph pattern matching.
 */
public class QueryHandler {
  /**
   * GDL handler
   */
  private final GDLHandler gdlHandler;
  /**
   * Graph diameter
   */
  private Integer diameter;
  /**
   * Graph radius
   */
  private Integer radius;
  /**
   * Graph components
   */
  private Map<Integer, Set<String>> components;
  /**
   * Cache: vId --> Vertex with Id == vId
   */
  private Map<Long, Vertex> idToVertexCache;
  /**
   * Cache: eId --> Edge with Id == eId
   */
  private Map<Long, Edge> idToEdgeCache;
  /**
   * Cache: l -> Vertices with Label == l
   */
  private Map<String, Set<Vertex>> labelToVertexCache;
  /**
   * Cache: l -> Edges with Label = l
   */
  private Map<String, Set<Edge>> labelToEdgeCache;
  /**
   * Cache: vId -> Edges with Source Id == vId
   */
  private Map<Long, Set<Edge>> sourceIdToEdgeCache;
  /**
   * Cache: vId -> Edges with Target Id == vId
   */
  private Map<Long, Set<Edge>> targetIdToEdgeCache;

  /**
   * Creates a new query handler.
   *
   * @param gdlString GDL query string
   */
  public QueryHandler(String gdlString) {
    gdlHandler = new GDLHandler.Builder()
      .setDefaultGraphLabel(GradoopConstants.DEFAULT_GRAPH_LABEL)
      .setDefaultVertexLabel(GradoopConstants.DEFAULT_VERTEX_LABEL)
      .setDefaultEdgeLabel(GradoopConstants.DEFAULT_EDGE_LABEL)
      .buildFromString(gdlString);
  }

  /**
   * Returns all vertices in the query.
   *
   * @return vertices
   */
  public Collection<Vertex> getVertices() {
    return gdlHandler.getVertices();
  }

  /**
   * Returns all edges in the query.
   *
   * @return edges
   */
  public Collection<Edge> getEdges() {
    return gdlHandler.getEdges();
  }

  /**
   * Returns the query graph as a collection of triples.
   *
   * @return triples
   */
  public Collection<Triple> getTriples() {
    return getEdges().stream()
      .map(e -> new Triple(
        getVertexById(e.getSourceVertexId()), e, getVertexById(e.getTargetVertexId())))
      .collect(Collectors.toList());
  }

  /**
   * Returns all variables contained in the pattern.
   *
   * @return all query variables
   */
  public Set<String> getAllVariables() {
    return Sets.union(getVertexVariables(), getEdgeVariables());
  }

  /**
   * Returns all vertex variables contained in the pattern.
   *
   * @return all vertex variables
   */
  public Set<String> getVertexVariables() {
    return gdlHandler.getVertexCache().keySet();
  }

  /**
   * Returns all edge variables contained in the pattern.
   *
   * @return all edge variables
   */
  public Set<String> getEdgeVariables() {
    return gdlHandler.getEdgeCache().keySet();
  }

  /**
   * Returns all available predicates in Conjunctive Normal Form {@link CNF}. If there are no
   * predicated defined in the query, a CNF containing zero predicates is returned.
   *
   * @return predicates
   */
  public CNF getPredicates() {
    if (gdlHandler.getPredicates().isPresent()) {
      return QueryPredicate.createFrom(gdlHandler.getPredicates().get()).asCNF();
    } else {
      return new CNF();
    }
  }

  /**
   * Returns the number of vertices in the query graph.
   *
   * @return vertex count
   */
  public int getVertexCount() {
    return getVertices().size();
  }

  /**
   * Returns the number of edges in the query graph.
   *
   * @return edge count
   */
  public int getEdgeCount() {
    return getEdges().size();
  }

  /**
   * Checks if the graph returns a single vertex and no edges (no loops).
   *
   * @return true, if single vertex graph
   */
  public boolean isSingleVertexGraph() {
    return getVertexCount() == 1 && getEdgeCount() == 0;
  }

  /**
   * Returns the diameter of the query graph.
   *
   * @return diameter
   */
  public int getDiameter() {
    if (diameter == null) {
      diameter = GraphMetrics.getDiameter(this);
    }
    return diameter;
  }

  /**
   * Returns the radius of the query graph.
   *
   * @return radius
   */
  public int getRadius() {
    if (radius == null) {
      radius = GraphMetrics.getRadius(this);
    }
    return radius;
  }

  /**
   * Returns the mapping of vertices to connected graph components
   *
   * @return connected components
   */
  public Map<Integer, Set<String>> getComponents() {
    if (components == null) {
      components = GraphMetrics.getComponents(this);
    }
    return components;
  }

  /**
   * Checks if the given variable points to a vertex.
   *
   * @param variable the elements variable
   * @return True if the variable points to a vertex
   */
  public boolean isVertex(String variable) {
    return gdlHandler.getVertexCache().containsKey(variable);
  }

  /**
   * Checks if the given variable points to an edge.
   *
   * @param variable the elements variable
   * @return True if the variable points to an edge
   */
  public boolean isEdge(String variable) {
    return gdlHandler.getEdgeCache().containsKey(variable);
  }

  /**
   * Returns the vertex associated with the given id or {@code null} if the
   * vertex does not exist.
   *
   * @param id vertex id
   * @return vertex or {@code null}
   */
  public Vertex getVertexById(Long id) {
    if (idToVertexCache == null) {
      idToVertexCache = initCache(getVertices(), Vertex::getId, Function.identity());
    }
    return idToVertexCache.get(id);
  }

  /**
   * Returns the edge associated with the given id or {@code null} if the
   * edge does not exist.
   *
   * @param id edge id
   * @return edge or {@code null}
   */
  public Edge getEdgeById(Long id) {
    if (idToEdgeCache == null) {
      idToEdgeCache = initCache(getEdges(), Edge::getId, Function.identity());
    }
    return idToEdgeCache.get(id);
  }

  /**
   * Returns the vertex associated with the given variable or {@code null} if the variable does
   * not exist. The variable can be either user-defined or auto-generated.
   *
   * @param variable query vertex variable
   * @return vertex or {@code null}
   */
  public Vertex getVertexByVariable(String variable) {
    return gdlHandler.getVertexCache(true, true).get(variable);
  }


  /**
   * Returns the Edge associated with the given variable or {@code null} if the variable does
   * not exist. The variable can be either user-defined or auto-generated.
   *
   * @param variable query edge variable
   * @return edge or {@code null}
   */
  public Edge getEdgeByVariable(String variable) {
    return gdlHandler.getEdgeCache(true, true).get(variable);
  }

  /**
   * Returns all vertices with the given label or {@code null} if there are no
   * vertices with that label.
   *
   * @param label vertex label
   * @return vertices or {@code null}
   */
  public Collection<Vertex> getVerticesByLabel(String label) {
    if (labelToVertexCache == null) {
      labelToVertexCache = initSetCache(getVertices(), Vertex::getLabel, Function.identity());
    }
    return labelToVertexCache.get(label);
  }

  /**
   * Returns all edges with the given label or {@code null} if there are no
   * edges with that label.
   *
   * @param label edge label
   * @return edges or {@code null}
   */
  public Collection<Edge> getEdgesByLabel(String label) {
    if (labelToEdgeCache == null) {
      labelToEdgeCache = initSetCache(getEdges(), Edge::getLabel, Function.identity());
    }
    return labelToEdgeCache.get(label);
  }

  /**
   * Returns all outgoing and incoming edges that are incident to the given
   * vertex id.
   *
   * @param vertexId vertex id
   * @return incoming and outgoing edges
   */
  public Collection<Edge> getEdgesByVertexId(Long vertexId) {
    List<Edge> result = Lists.newArrayList();
    if (getEdgesBySourceVertexId(vertexId) != null) {
      result.addAll(getEdgesBySourceVertexId(vertexId));
    }
    if (getEdgesByTargetVertexId(vertexId) != null) {
      result.addAll(getEdgesByTargetVertexId(vertexId));
    }
    return result;
  }

  /**
   * Returns all edges that start at the given vertex id or {@code null} if the
   * given vertex has no outgoing edges.
   *
   * @param vertexId vertex id
   * @return outgoing edges or {@code null}
   */
  public Collection<Edge> getEdgesBySourceVertexId(Long vertexId) {
    if (sourceIdToEdgeCache == null) {
      sourceIdToEdgeCache = initSetCache(getEdges(), Edge::getSourceVertexId, Function.identity());
    }
    return sourceIdToEdgeCache.get(vertexId);
  }

  /**
   * Returns all edge ids that start at the given vertex id or {@code null} if
   * the given vertex has no outgoing edges.
   *
   * @param vertexId vertex id
   * @return outgoing edge ids or {@code null}
   */
  public Collection<Long> getEdgeIdsBySourceVertexId(Long vertexId) {
    return getIds(getEdgesBySourceVertexId(vertexId));
  }

  /**
   * Returns all edges that point to the given vertex id or {@code null} if the
   * given vertex has no incoming edges.
   *
   * @param vertexId vertex id
   * @return incoming edges or {@code null}
   */
  public Collection<Edge> getEdgesByTargetVertexId(Long vertexId) {
    if (targetIdToEdgeCache == null) {
      targetIdToEdgeCache = initSetCache(getEdges(), Edge::getTargetVertexId, Function.identity());
    }
    return targetIdToEdgeCache.get(vertexId);
  }

  /**
   * Returns all edge ids that point to the given vertex id or {@code null} if
   * the given vertex has no incoming edges.
   *
   * @param vertexId vertex id
   * @return incoming edge ids or {@code null}
   */
  public Collection<Long> getEdgeIdsByTargetVertexId(Long vertexId) {
    return getIds(getEdgesByTargetVertexId(vertexId));
  }

  /**
   * Returns all edges that point to the source vertex of the given edge or
   * {@code null} if the edge has no predecessors.
   *
   * @param edgeId edge id
   * @return preceding edges or {@code null}
   */
  public Collection<Edge> getPredecessors(Long edgeId) {
    Collection<Edge> predecessors =
      getEdgesByTargetVertexId(getEdgeById(edgeId).getSourceVertexId());
    return predecessors != null ?
      Lists.newArrayList(predecessors) : Lists.<Edge>newArrayList();
  }

  /**
   * Returns all edges that start at the target vertex of the given edge or
   * {@code null} if the edge has no successors.
   *
   * @param edgeId edge id
   * @return succeeding edges or {@code null}
   */
  public Collection<Edge> getSuccessors(Long edgeId) {
    Collection<Edge> successors =
      getEdgesBySourceVertexId(getEdgeById(edgeId).getTargetVertexId());
    return successors != null ?
      Lists.newArrayList(successors) : Lists.<Edge>newArrayList();
  }

  /**
   * Returns all edge ids that point to the source vertex of the given edge or
   * {@code null} if the edge has no predecessors.
   *
   * @param edgeId edge id
   * @return preceding edge ids or {@code null}
   */
  public Collection<Long> getPredecessorIds(Long edgeId) {
    return getIds(getPredecessors(edgeId));
  }

  /**
   * Returns all edge ids that start at the target vertex of the given edge or
   * {@code null} if the edge has no successors.
   *
   * @param edgeId edge id
   * @return succeeding edge ids or {@code null}
   */
  public Collection<Long> getSuccessorIds(Long edgeId) {
    return getIds(getSuccessors(edgeId));
  }

  /**
   * Returns all vertices that are adjacent to the given vertex (without
   * considering edge direction).
   *
   * @param vertexId vertex id
   * @return adjacent vertices
   */
  public Collection<Vertex> getNeighbors(Long vertexId) {
    Set<Vertex> neighbors = Sets.newHashSet();
    Collection<Edge> outgoingEdges = getEdgesBySourceVertexId(vertexId);
    if (outgoingEdges != null) {
      for (Edge edge : outgoingEdges) {
        neighbors.add(getVertexById(edge.getTargetVertexId()));
      }
    }
    Collection<Edge> incomingEdges = getEdgesByTargetVertexId(vertexId);
    if (incomingEdges != null) {
      for (Edge edge : incomingEdges) {
        neighbors.add(getVertexById(edge.getSourceVertexId()));
      }
    }
    return neighbors;
  }

  /**
   * Returns all vertex ids that are adjacent to the given vertex (without
   * considering edge direction).
   *
   * @param vertexId vertex id
   * @return adjacent vertex ids
   */
  public Collection<Long> getNeighborIds(Long vertexId) {
    Set<Long> neighbors = Sets.newHashSet();
    for (Vertex vertex : getNeighbors(vertexId)) {
      neighbors.add(vertex.getId());
    }
    return neighbors;
  }

  /**
   * Returns a mapping between the given variables (if existent) and the corresponding element
   * label.
   *
   * @param variables query variables
   * @return mapping between existing variables and their corresponding label
   */
  public Map<String, String> getLabelsForVariables(Collection<String> variables) {
    return variables.stream()
      .filter(var -> isEdge(var) || isVertex(var))
      .map(var -> {
          if (isEdge(var)) {
            return Pair.of(var, getEdgeByVariable(var).getLabel());
          } else {
            return Pair.of(var, getVertexByVariable(var).getLabel());
          }
        }).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
  }

  /**
   * Returns a mapping from edge variable to the corresponding source and target variables.
   *
   * @return mapping from edge variable to source/target variable
   */
  public Map<String, Pair<String, String>> getSourceTargetVariables() {
    return getEdges().stream()
      .map(e -> Pair.of(e.getVariable(), Pair
        .of(getVertexById(e.getSourceVertexId()).getVariable(),
          getVertexById(e.getTargetVertexId()).getVariable())))
      .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
  }

  /**
   * Returns the ids of the given graph elements (vertex/edge).
   *
   * @param elements graph elements
   * @param <EL> element type
   * @return element identifiers
   */
  private <EL extends GraphElement> Collection<Long>
  getIds(Collection<EL> elements) {
    List<Long> ids = null;
    if (elements != null) {
      ids = Lists.newArrayListWithCapacity(elements.size());
      for (EL el : elements) {
        ids.add(el.getId());
      }
    }
    return ids;
  }

  /**
   * Returns all vertices that have a minimum eccentricity.
   *
   * @return center vertices
   */
  public Collection<Vertex> getCenterVertices() {
    List<Vertex> result = Lists.newArrayList();
    for (Long vId : getCentralVertexIds()) {
      result.add(getVertexById(vId));
    }
    return result;
  }

  /**
   * Returns a single center vertex of the query graph.
   *
   * @return center vertex
   */
  public Vertex getCenterVertex() {
    return getCenterVertices().iterator().next();
  }

  /**
   * Returns all vertex ids that have a minimum eccentricity.
   *
   * @return vertex identifiers
   */
  public Collection<Long> getCentralVertexIds() {
    Map<Long, Integer> eccMap = GraphMetrics.getEccentricity(this);
    Integer min = Collections.min(eccMap.values());
    List<Long> result = Lists.newArrayList();
    for (Map.Entry<Long, Integer> eccEntry : eccMap.entrySet()) {
      if (eccEntry.getValue().equals(min)) {
        result.add(eccEntry.getKey());
      }
    }
    return result;
  }

  /**
   * Initializes a cache for the given data where every key maps to exactly one element (injective).
   * Key selector will be called on every element to extract the caches key.
   * Value selector will be called on every element to extract the value.
   * Returns a cache of type
   * KT -> VT
   *
   * @param elements elements the cache will be build from
   * @param keySelector key selector function extraction cache keys from elements
   * @param valueSelector value selector function extraction cache values from elements
   * @param <EL> the element type
   * @param <KT> the cache key type
   * @param <VT> the cache value type
   * @return cache KT -> VT
   */
  private <EL, KT, VT> Map<KT, VT> initCache(Collection<EL> elements,
    Function<EL, KT> keySelector, Function<EL, VT> valueSelector) {

    return elements.stream().collect(Collectors.toMap(keySelector, valueSelector));
  }

  /**
   * Initializes a cache for the given elements where every key maps to multiple elements.
   * Key selector will be called on every element to extract the caches key.
   * Value selector will be called on every element to extract the value.
   * Returns a cache of the form
   * KT -> Set<VT>
   *
   * @param elements elements the cache will be build from
   * @param keySelector key selector function extraction cache keys from elements
   * @param valueSelector value selector function extraction cache values from elements
   * @param <EL> the element type
   * @param <KT> the cache key type
   * @param <VT> the cache value type
   * @return cache KT -> Set<VT>
   */
  private <EL, KT, VT> Map<KT, Set<VT>> initSetCache(Collection<EL> elements,
    Function<EL, KT> keySelector, Function<EL, VT> valueSelector) {

    return elements.stream()
      .collect(
        Collectors.groupingBy(
          keySelector,
          Collectors.mapping(valueSelector, Collectors.toSet())
      ));
  }
}
