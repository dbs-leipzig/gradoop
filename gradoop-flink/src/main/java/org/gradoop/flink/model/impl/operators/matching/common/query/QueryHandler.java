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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.gradoop.common.util.GConstants;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.GraphElement;
import org.s1ck.gdl.model.Vertex;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
      .setDefaultGraphLabel(GConstants.DEFAULT_GRAPH_LABEL)
      .setDefaultVertexLabel(GConstants.DEFAULT_VERTEX_LABEL)
      .setDefaultEdgeLabel(GConstants.DEFAULT_EDGE_LABEL)
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
   * Returns the vertex associated with the given id or {@code null} if the
   * vertex does not exist.
   *
   * @param id vertex id
   * @return vertex or {@code null}
   */
  public Vertex getVertexById(Long id) {
    if (idToVertexCache == null) {
      idToVertexCache = initIdToElementCache(gdlHandler.getVertices());
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
      idToEdgeCache = initIdToElementCache(gdlHandler.getEdges());
    }
    return idToEdgeCache.get(id);
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
      initLabelToVertexCache();
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
      initLabelToEdgeCache();
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
      initSourceIdToEdgeCache();
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
      initTargetIdToEdgeCache();
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
   * Initializes the identifier to graph element (vertex/edge) cache.
   *
   * @param elements graph elements
   * @param <EL> element type
   * @return id to element cache
   */
  private <EL extends GraphElement> Map<Long, EL>
  initIdToElementCache(Collection<EL> elements) {
    Map<Long, EL> cache = Maps.newHashMap();
    for (EL el : elements) {
      cache.put(el.getId(), el);
    }
    return cache;
  }

  /**
   * Initializes {@link QueryHandler#labelToVertexCache}.
   */
  private void initLabelToVertexCache() {
    labelToVertexCache = initLabelToGraphElementCache(getVertices());
  }

  /**
   * Initializes {@link QueryHandler#labelToEdgeCache}.
   */
  private void initLabelToEdgeCache() {
    labelToEdgeCache = initLabelToGraphElementCache(getEdges());
  }

  /**
   * Initializes {@link QueryHandler#sourceIdToEdgeCache}.
   */
  private void initSourceIdToEdgeCache() {
    sourceIdToEdgeCache = initVertexToEdgeCache(gdlHandler.getEdges(), true);
  }

  /**
   * Initializes {@link QueryHandler#targetIdToEdgeCache}.
   */
  private void initTargetIdToEdgeCache() {
    targetIdToEdgeCache = initVertexToEdgeCache(gdlHandler.getEdges(), false);
  }

  /**
   * Initializes vertex id to edge cache.
   *
   * @param edges query edges
   * @param useSource use source (true) or target (false) vertex for caching
   * @return vertex id to edge cache
   */
  private Map<Long, Set<Edge>>
  initVertexToEdgeCache(Collection<Edge> edges, boolean useSource) {
    Map<Long, Set<Edge>> cache = Maps.newHashMap();
    for (Edge e : edges) {
      Long vId = useSource ? e.getSourceVertexId() : e.getTargetVertexId();

      if (cache.containsKey(vId)) {
        cache.get(vId).add(e);
      } else {
        cache.put(vId, Sets.newHashSet(e));
      }
    }
    return cache;
  }

  /**
   * Initializes label to graph element (vertex/edge) cache.
   *
   * @param elements graph elements
   * @param <EL> element type
   * @return label to element cache
   */
  private <EL extends GraphElement> Map<String, Set<EL>>
  initLabelToGraphElementCache(Collection<EL> elements) {
    Map<String, Set<EL>> cache = Maps.newHashMap();
    for (EL el : elements) {
      if (cache.containsKey(el.getLabel())) {
        cache.get(el.getLabel()).add(el);
      } else {
        Set<EL> set = new HashSet<>();
        set.add(el);
        cache.put(el.getLabel(), set);
      }
    }
    return cache;
  }
}
