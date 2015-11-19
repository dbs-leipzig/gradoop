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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIds;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.Graph;
import org.s1ck.gdl.model.Vertex;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Creates collections of graphs, vertices and edges from a given GDL script.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 * @param <G> EPGM graph type
 *
 * @see <a href="https://github.com/s1ck/gdl">GDL on GitHub</a>
 */
public class AsciiGraphLoader<
  V extends EPGMVertex,
  E extends EPGMEdge,
  G extends EPGMGraphHead> {

  /**
   * Gradoop configuration
   */
  private final GradoopConfig<V, E, G> config;

  /**
   * Used to parse GDL scripts.
   */
  private final GDLHandler gdlHandler;

  /**
   * Stores all graphs contained in the GDL script.
   */
  private final Map<GradoopId, G> graphHeads;
  /**
   * Stores all vertices contained in the GDL script.
   */
  private final Map<GradoopId, V> vertices;
  /**
   * Stores all edges contained in the GDL script.
   */
  private final Map<GradoopId, E> edges;


  /**
   * Stores graphs that are assigned to a variable.
   */
  private final Map<String, G> graphHeadCache;
  /**
   * Stores vertices that are assigned to a variable.
   */
  private final Map<String, V> vertexCache;
  /**
   * Stores edges that are assigned to a variable.
   */
  private final Map<String, E> edgeCache;

  /**
   * Creates a new AsciiGraphLoader.
   *
   * @param gdlHandler GDL Handler
   * @param config Gradoop configuration
   */
  private AsciiGraphLoader(GDLHandler gdlHandler,
    GradoopConfig<V, E, G> config) {
    this.gdlHandler = gdlHandler;
    this.config = config;

    this.graphHeads = Maps.newHashMap();
    this.vertices = Maps.newHashMap();
    this.edges = Maps.newHashMap();

    this.graphHeadCache = Maps.newHashMap();
    this.vertexCache = Maps.newHashMap();
    this.edgeCache = Maps.newHashMap();

    init();
  }

  /**
   * Creates an AsciiGraphLoader from the given ASCII GDL string.
   *
   * @param asciiGraph  GDL string
   * @param config      Gradoop configuration
   * @param <V>         EPGM vertex type
   * @param <E>         EPGM edge type
   * @param <G>         EPGM graph type
   * @return AsciiGraphLoader
   */
  public static
  <V extends EPGMVertex, E extends EPGMEdge, G extends EPGMGraphHead>
  AsciiGraphLoader<V, E, G> fromString(String asciiGraph,
    GradoopConfig<V, E, G> config) {
    return new AsciiGraphLoader<>(GDLHandler.initFromString(asciiGraph),
      config);
  }

  /**
   * Creates an AsciiGraphLoader from the given ASCII GDL file.
   *
   * @param fileName  File that contains a GDL script
   * @param config    Gradoop configuration
   * @param <V>       EPGM vertex type
   * @param <E>       EPGM edge type
   * @param <G>       EPGM graph type
   * @return AsciiGraphLoader
   * @throws IOException
   */
  public static
  <V extends EPGMVertex, E extends EPGMEdge, G extends EPGMGraphHead>
  AsciiGraphLoader<V, E, G> fromFile(String fileName,
    GradoopConfig<V, E, G> config) throws IOException {
    return new AsciiGraphLoader<>(GDLHandler.initFromFile(fileName), config);
  }

  // ---------------------------------------------------------------------------
  //  Graph methods
  // ---------------------------------------------------------------------------

  /**
   * Returns all GraphHeads contained in the ASCII graph.
   *
   * @return graphHeads
   */
  public Collection<G> getGraphHeads() {
    return new ImmutableSet.Builder<G>().addAll(graphHeads.values()).build();
  }

  /**
   * Returns GraphHead by given variable.
   *
   * @param variable variable used in GDL script
   * @return graphHead or {@code null} if graph is not cached
   */
  public G getGraphHeadByVariable(String variable) {
    return getGraphHeadCache().get(variable);
  }

  /**
   * Returns GraphHeads by their given variables.
   *
   * @param variables variables used in GDL script
   * @return graphHeads that are assigned to the given variables
   */
  public Collection<G> getGraphHeadsByVariables(String... variables) {
    Collection<G> graphHeads =
      Sets.newHashSetWithExpectedSize(variables.length);
    for (String variable : variables) {
      G graphHead = getGraphHeadByVariable(variable);
      if (graphHead != null) {
        graphHeads.add(graphHead);
      }
    }
    return graphHeads;
  }

  // ---------------------------------------------------------------------------
  //  Vertex methods
  // ---------------------------------------------------------------------------

  /**
   * Returns all vertices contained in the ASCII graph.
   *
   * @return vertices
   */
  public Collection<V> getVertices() {
    return new ImmutableSet.Builder<V>().addAll(vertices.values()).build();
  }

  /**
   * Returns vertices by their given variables.
   *
   * @param variables variables used in GDL script
   * @return vertices
   */
  public Collection<V> getVerticesByVariables(String... variables) {
    Collection<V> vertices = Sets.newHashSetWithExpectedSize(variables.length);
    for (String variable : variables) {
      V vertex = vertexCache.get(variable);
      if (vertex != null) {
        vertices.add(vertex);
      }
    }
    return vertices;
  }

  /**
   * Returns all vertices that belong to the given graphs.
   *
   * @param graphIds graph identifiers
   * @return vertices that are contained in the graphs
   */
  public Collection<V> getVerticesByGraphIds(GradoopIds graphIds) {
    Collection<V> result = Sets.newHashSetWithExpectedSize(graphIds.size());
    for (V vertex : vertices.values()) {
      if (vertex.getGraphIds().containsAll(graphIds)) {
        result.add(vertex);
      }
    }
    return result;
  }

  /**
   * Returns all vertices that belong to the given graph variables.
   *
   * @param graphVariables graph variables used in the GDL script
   * @return vertices that are contained in the graphs
   */
  public Collection<V> getVerticesByGraphVariables(String... graphVariables) {
    GradoopIds graphIds = new GradoopIds();
    Collection<G> graphHeads = getGraphHeadsByVariables(graphVariables);
    for (G graphHead : graphHeads) {
      graphIds.add(graphHead.getId());
    }
    return getVerticesByGraphIds(graphIds);
  }

  // ---------------------------------------------------------------------------
  //  Edge methods
  // ---------------------------------------------------------------------------

  /**
   * Returns all edges contained in the ASCII graph.
   *
   * @return edges
   */
  public Collection<E> getEdges() {
    return new ImmutableSet.Builder<E>().addAll(edges.values()).build();
  }

  /**
   * Returns edges by their given variables.
   *
   * @param variables variables used in GDL script
   * @return edges
   */
  public Collection<E> getEdgesByVariables(String... variables) {
    Collection<E> edges = Sets.newHashSetWithExpectedSize(variables.length);
    for (String variable : variables) {
      E edge = edgeCache.get(variable);
      if (edge != null) {
        edges.add(edge);
      }
    }
    return edges;
  }

  /**
   * Returns all edges that belong to the given graphs.
   *
   * @param graphIds Graph identifiers
   * @return edges
   */
  public Collection<E> getEdgesByGraphIds(GradoopIds graphIds) {
    Collection<E> result = Sets.newHashSetWithExpectedSize(graphIds.size());
    for (E edge : edges.values()) {
      if (edge.getGraphIds().containsAll(graphIds)) {
        result.add(edge);
      }
    }
    return result;
  }

  /**
   * Returns all edges that belong to the given graph variables.
   *
   * @param variables graph variables used in the GDL script
   * @return edges
   */
  public Collection<E> getEdgesByGraphVariables(String... variables) {
    GradoopIds graphIds = new GradoopIds();
    Collection<G> graphHeads = getGraphHeadsByVariables(variables);
    for (G graphHead : graphHeads) {
      graphIds.add(graphHead.getId());
    }
    return getEdgesByGraphIds(graphIds);
  }

  // ---------------------------------------------------------------------------
  //  Caches
  // ---------------------------------------------------------------------------

  /**
   * Returns all graph heads that are bound to a variable in the GDL script.
   *
   * @return variable to graphHead mapping
   */
  public Map<String, G> getGraphHeadCache() {
    return new ImmutableMap.Builder<String, G>().putAll(graphHeadCache).build();
  }

  /**
   * Returns all vertices that are bound to a variable in the GDL script.
   *
   * @return variable to vertex mapping
   */
  public Map<String, V> getVertexCache() {
    return new ImmutableMap.Builder<String, V>().putAll(vertexCache).build();
  }

  /**
   * Returns all edges that are bound to a variable in the GDL script.
   *
   * @return variable to edge mapping
   */
  public Map<String, E> getEdgeCache() {
    return new ImmutableMap.Builder<String, E>().putAll(edgeCache).build();
  }

  // ---------------------------------------------------------------------------
  //  Private init methods
  // ---------------------------------------------------------------------------

  /**
   * Initializes the AsciiGraphLoader
   */
  private void init() {
    initGraphHeads();
    initVertices();
    initEdges();
  }

  /**
   * Initializes GraphHeads and their cache.
   */
  private void initGraphHeads() {
    for (Graph g : gdlHandler.getGraphs()) {
      G graphHead = initGraphHead(g);
      graphHeads.put(graphHead.getId(), graphHead);
    }

    for (Map.Entry<String, Graph> e : gdlHandler.getGraphCache().entrySet()) {
      updateGraphCache(e.getKey(), e.getValue());
    }
  }

  /**
   * Initializes vertices and their cache.
   */
  private void initVertices() {
    for (Vertex v : gdlHandler.getVertices()) {
      V vertex = initVertex(v);
      vertices.put(vertex.getId(), vertex);
    }

    for (Map.Entry<String, Vertex> e : gdlHandler.getVertexCache().entrySet()) {
      updateVertexCache(e.getKey(), e.getValue());
    }
  }

  /**
   * Initializes edges and their cache.
   */
  private void initEdges() {
    for (Edge e : gdlHandler.getEdges()) {
      E edge = initEdge(e);
      edges.put(edge.getId(), edge);
    }

    for (Map.Entry<String, Edge> e : gdlHandler.getEdgeCache().entrySet()) {
      updateEdgeCache(e.getKey(), e.getValue());
    }
  }

  /**
   * Creates a new EPGMGraph from the GDL Loader.
   *
   * @param g graph from GDL Loader
   * @return EPGM GraphHead
   */
  private G initGraphHead(Graph g) {
    GradoopId graphId = GradoopId.fromLong(g.getId());
    return config.getGraphHeadFactory().createGraphHead(graphId,
      g.getLabel(),
      g.getProperties());
  }

  /**
   * Creates a new EPGMVertex from the GDL Loader.
   *
   * @param v vertex from GDL Loader
   * @return EPGM Vertex
   */
  private V initVertex(Vertex v) {
    GradoopId vertexId = GradoopId.fromLong(v.getId());
    GradoopIds graphIds = GradoopIds.fromLongs(v.getGraphs());
    return config.getVertexFactory().createVertex(vertexId,
      v.getLabel(),
      v.getProperties(),
      graphIds);
  }

  /**
   * Creates a new EPGMEdge from the GDL Loader.
   *
   * @param e edge from GDL loader
   * @return EPGM edge
   */
  private E initEdge(Edge e) {
    GradoopId edgeId = GradoopId.fromLong(e.getId());
    GradoopId sourceVertexId = GradoopId.fromLong(e.getSourceVertexId());
    GradoopId targetVertexId = GradoopId.fromLong(e.getTargetVertexId());
    GradoopIds graphIds = GradoopIds.fromLongs(e.getGraphs());
    return config.getEdgeFactory().createEdge(edgeId,
      e.getLabel(),
      sourceVertexId,
      targetVertexId,
      e.getProperties(),
      graphIds);
  }

  /**
   * Updates the graph cache.
   *
   * @param variable graph variable used in GDL script
   * @param g graph from GDL loader
   */
  private void updateGraphCache(String variable, Graph g) {
    GradoopId graphId = GradoopId.fromLong(g.getId());
    graphHeadCache.put(variable, graphHeads.get(graphId));
  }

  /**
   * Updates the vertex cache.
   *
   * @param variable vertex variable used in GDL script
   * @param v vertex from GDL loader
   */
  private void updateVertexCache(String variable, Vertex v) {
    GradoopId vertexId = GradoopId.fromLong(v.getId());
    vertexCache.put(variable, vertices.get(vertexId));
  }

  /**
   * Updates the edge cache.
   *
   * @param variable edge variable used in the GDL script
   * @param e edge from GDL loader
   */
  private void updateEdgeCache(String variable, Edge e) {
    GradoopId edgeId = GradoopId.fromLong(e.getId());
    edgeCache.put(variable, edges.get(edgeId));
  }
}
