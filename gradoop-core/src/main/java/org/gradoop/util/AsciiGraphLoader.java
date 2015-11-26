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
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.id.ImportIdGenerator;
import org.gradoop.model.impl.id.ReuseIdGenerator;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.Graph;
import org.s1ck.gdl.model.GraphElement;
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
public class AsciiGraphLoader
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge> {

  /**
   * Gradoop configuration
   */
  private final GradoopConfig<G, V, E> config;

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
   * Used to create GradoopIds from existing identifiers.
   */
  private final ReuseIdGenerator idGenerator;

  /**
   * Creates a new AsciiGraphLoader.
   *
   * @param gdlHandler GDL Handler
   * @param config Gradoop configuration
   */
  private AsciiGraphLoader(GDLHandler gdlHandler,
    GradoopConfig<G, V, E> config) {
    this.gdlHandler = gdlHandler;
    this.config = config;

    this.graphHeads = Maps.newHashMap();
    this.vertices = Maps.newHashMap();
    this.edges = Maps.newHashMap();

    this.graphHeadCache = Maps.newHashMap();
    this.vertexCache = Maps.newHashMap();
    this.edgeCache = Maps.newHashMap();

    this.idGenerator = new ImportIdGenerator();

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
  AsciiGraphLoader<G, V, E> fromString(String asciiGraph,
    GradoopConfig<G, V, E> config) {
    return new AsciiGraphLoader<>(new GDLHandler.Builder()
      .setDefaultGraphLabel(GConstants.DEFAULT_GRAPH_LABEL)
      .setDefaultVertexLabel(GConstants.DEFAULT_VERTEX_LABEL)
      .setDefaultEdgeLabel(GConstants.DEFAULT_EDGE_LABEL)
      .buildFromString(asciiGraph),
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
  AsciiGraphLoader<G, V, E> fromFile(String fileName,
    GradoopConfig<G, V, E> config) throws IOException {
    return new AsciiGraphLoader<>(new GDLHandler.Builder()
      .setDefaultGraphLabel(GConstants.DEFAULT_GRAPH_LABEL)
      .setDefaultVertexLabel(GConstants.DEFAULT_VERTEX_LABEL)
      .setDefaultEdgeLabel(GConstants.DEFAULT_EDGE_LABEL)
      .buildFromFile(fileName),
      config);
  }

  /**
   * Appends the given ASCII GDL to the graph handled by that loader.
   *
   * Variables that were previously used, can be reused in the given script and
   * refer to the same entities.
   *
   * @param asciiGraph GDL string
   */
  public void appendFromString(String asciiGraph) {
    this.gdlHandler.append(asciiGraph);
    init();
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
    Collection<G> result =
      Sets.newHashSetWithExpectedSize(variables.length);
    for (String variable : variables) {
      G graphHead = getGraphHeadByVariable(variable);
      if (graphHead != null) {
        result.add(graphHead);
      }
    }
    return result;
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
   * Returns vertex by its given variable.
   *
   * @param variable variable used in GDL script
   * @return vertex or {@code null} if not present
   */
  public V getVertexByVariable(String variable) {
    return vertexCache.get(variable);
  }

  /**
   * Returns vertices by their given variables.
   *
   * @param variables variables used in GDL script
   * @return vertices
   */
  public Collection<V> getVerticesByVariables(String... variables) {
    Collection<V> result = Sets.newHashSetWithExpectedSize(variables.length);
    for (String variable : variables) {
      V vertex = getVertexByVariable(variable);
      if (vertex != null) {
        result.add(vertex);
      }
    }
    return result;
  }

  /**
   * Returns all vertices that belong to the given graphs.
   *
   * @param graphIds graph identifiers
   * @return vertices that are contained in the graphs
   */
  public Collection<V> getVerticesByGraphIds(GradoopIdSet graphIds) {
    Collection<V> result = Sets.newHashSetWithExpectedSize(graphIds.size());
    for (V vertex : vertices.values()) {
      if (vertex.getGraphIds().containsAny(graphIds)) {
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
    GradoopIdSet graphIds = new GradoopIdSet();
    for (G graphHead : getGraphHeadsByVariables(graphVariables)) {
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
   * Returns edge by its given variable.
   *
   * @param variable variable used in GDL script
   * @return edge or {@code null} if not present
   */
  public E getEdgeByVariable(String variable) {
    return edgeCache.get(variable);
  }

  /**
   * Returns edges by their given variables.
   *
   * @param variables variables used in GDL script
   * @return edges
   */
  public Collection<E> getEdgesByVariables(String... variables) {
    Collection<E> result = Sets.newHashSetWithExpectedSize(variables.length);
    for (String variable : variables) {
      E edge = edgeCache.get(variable);
      if (edge != null) {
        result.add(edge);
      }
    }
    return result;
  }

  /**
   * Returns all edges that belong to the given graphs.
   *
   * @param graphIds Graph identifiers
   * @return edges
   */
  public Collection<E> getEdgesByGraphIds(GradoopIdSet graphIds) {
    Collection<E> result = Sets.newHashSetWithExpectedSize(graphIds.size());
    for (E edge : edges.values()) {
      if (edge.getGraphIds().containsAny(graphIds)) {
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
    GradoopIdSet graphIds = new GradoopIdSet();
    for (G graphHead : getGraphHeadsByVariables(variables)) {
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
    return config.getGraphHeadFactory().initGraphHead(
      idGenerator.createId(g.getId()),
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
    return config.getVertexFactory().initVertex(
      idGenerator.createId(v.getId()),
      v.getLabel(),
      v.getProperties(),
      createGradoopIdSet(v));
  }

  /**
   * Creates a new EPGMEdge from the GDL Loader.
   *
   * @param e edge from GDL loader
   * @return EPGM edge
   */
  private E initEdge(Edge e) {
    return config.getEdgeFactory().initEdge(
      idGenerator.createId(e.getId()),
      e.getLabel(),
      idGenerator.createId(e.getSourceVertexId()),
      idGenerator.createId(e.getTargetVertexId()),
      e.getProperties(),
      createGradoopIdSet(e));
  }

  /**
   * Updates the graph cache.
   *
   * @param variable graph variable used in GDL script
   * @param g graph from GDL loader
   */
  private void updateGraphCache(String variable, Graph g) {
    graphHeadCache.put(
      variable, graphHeads.get(idGenerator.createId(g.getId())));
  }

  /**
   * Updates the vertex cache.
   *
   * @param variable vertex variable used in GDL script
   * @param v vertex from GDL loader
   */
  private void updateVertexCache(String variable, Vertex v) {
    vertexCache.put(variable, vertices.get(idGenerator.createId(v.getId())));
  }

  /**
   * Updates the edge cache.
   *
   * @param variable edge variable used in the GDL script
   * @param e edge from GDL loader
   */
  private void updateEdgeCache(String variable, Edge e) {
    edgeCache.put(variable, edges.get(idGenerator.createId(e.getId())));
  }

  /**
   * Creates a {@code GradoopIDSet} from the long identifiers stored at the
   * given graph element.
   *
   * @param e graph element
   * @return GradoopIDSet for the given element
   */
  private GradoopIdSet createGradoopIdSet(GraphElement e) {
    GradoopIdSet result = new GradoopIdSet();
    for (Long graphId : e.getGraphs()) {
      result.add(idGenerator.createId(graphId));
    }
    return result;
  }
}
