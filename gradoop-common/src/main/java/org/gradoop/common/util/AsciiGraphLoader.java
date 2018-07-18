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
package org.gradoop.common.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.config.GradoopConfig;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.Graph;
import org.s1ck.gdl.model.GraphElement;
import org.s1ck.gdl.model.Vertex;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;

/**
 * Creates collections of graphs, vertices and edges from a given GDL script.
 *
 * @see <a href="https://github.com/s1ck/gdl">GDL on GitHub</a>
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
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
   * Mapping between GDL ids and Gradoop IDs.
   */
  private final Map<Long, GradoopId> graphHeadIds;
  /**
   * Stores all vertices contained in the GDL script.
   */
  private final Map<GradoopId, V> vertices;
  /**
   * Mapping between GDL ids and Gradoop IDs.
   */
  private final Map<Long, GradoopId> vertexIds;
  /**
   * Stores all edges contained in the GDL script.
   */
  private final Map<GradoopId, E> edges;
  /**
   * Mapping between GDL ids and Gradoop IDs.
   */
  private final Map<Long, GradoopId> edgeIds;

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
    GradoopConfig<G, V, E> config) {
    this.gdlHandler = gdlHandler;
    this.config = config;

    this.graphHeads     = Maps.newHashMap();
    this.vertices       = Maps.newHashMap();
    this.edges          = Maps.newHashMap();

    this.graphHeadIds   = Maps.newHashMap();
    this.vertexIds      = Maps.newHashMap();
    this.edgeIds        = Maps.newHashMap();

    this.graphHeadCache = Maps.newHashMap();
    this.vertexCache    = Maps.newHashMap();
    this.edgeCache      = Maps.newHashMap();

    init();
  }

  /**
   * Creates an AsciiGraphLoader from the given ASCII GDL string.
   *
   * @param asciiGraph  GDL string
   * @param config      Gradoop configuration
   * @param <G> EPGM graph head type
   * @param <V> EPGM vertex type
   * @param <E> EPGM edge type
   *
   * @return AsciiGraphLoader
   */
  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  AsciiGraphLoader<G, V, E> fromString(String asciiGraph,
    GradoopConfig<G, V, E> config) {
    return new AsciiGraphLoader<>(new GDLHandler.Builder()
      .setDefaultGraphLabel(GradoopConstants.DEFAULT_GRAPH_LABEL)
      .setDefaultVertexLabel(GradoopConstants.DEFAULT_VERTEX_LABEL)
      .setDefaultEdgeLabel(GradoopConstants.DEFAULT_EDGE_LABEL)
      .buildFromString(asciiGraph),
      config);
  }

  /**
   * Creates an AsciiGraphLoader from the given ASCII GDL file.
   *
   * @param fileName  File that contains a GDL script
   * @param config    Gradoop configuration
   * @param <G> EPGM graph head type
   * @param <V> EPGM vertex type
   * @param <E> EPGM edge type
   *
   * @return AsciiGraphLoader
   * @throws IOException
   */
  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  AsciiGraphLoader<G, V, E> fromFile(String fileName,
    GradoopConfig<G, V, E> config) throws IOException {
    return new AsciiGraphLoader<>(new GDLHandler.Builder()
      .setDefaultGraphLabel(GradoopConstants.DEFAULT_GRAPH_LABEL)
      .setDefaultVertexLabel(GradoopConstants.DEFAULT_VERTEX_LABEL)
      .setDefaultEdgeLabel(GradoopConstants.DEFAULT_EDGE_LABEL)
      .buildFromFile(fileName),
      config);
  }

  /**
   * Creates an AsciiGraphLoader from the given ASCII GDL file.
   *
   * @param inputStream   File that contains a GDL script
   * @param config        Gradoop configuration
   * @param <G> EPGM graph head type
   * @param <V> EPGM vertex type
   * @param <E> EPGM edge type
   *
   * @return AsciiGraphLoader
   * @throws IOException
   */
  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  AsciiGraphLoader<G, V, E> fromStream(InputStream inputStream,
    GradoopConfig<G, V, E> config) throws IOException {
    return new AsciiGraphLoader<>(new GDLHandler.Builder()
      .setDefaultGraphLabel(GradoopConstants.DEFAULT_GRAPH_LABEL)
      .setDefaultVertexLabel(GradoopConstants.DEFAULT_VERTEX_LABEL)
      .setDefaultEdgeLabel(GradoopConstants.DEFAULT_EDGE_LABEL)
      .buildFromStream(inputStream),
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
    return new ImmutableSet.Builder<G>()
      .addAll(graphHeads.values()).build();
  }

  /**
   * Returns EPGMGraphHead by given variable.
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
  public Collection<G>  getGraphHeadsByVariables(String... variables) {
    Collection<G>  result =
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
  //  EPGMVertex methods
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
  //  EPGMEdge methods
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
    Collection<E>  result = Sets.newHashSetWithExpectedSize(variables.length);
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
  public Collection<E>  getEdgesByGraphIds(GradoopIdSet graphIds) {
    Collection<E>  result = Sets.newHashSetWithExpectedSize(graphIds.size());
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
  public Collection<E>  getEdgesByGraphVariables(String... variables) {
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
    return new ImmutableMap.Builder<String, G>().putAll(graphHeadCache)
      .build();
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
      if (!graphHeadIds.containsKey(g.getId())) {
        initGraphHead(g);
      }
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
      initVertex(v);
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
      initEdge(e);
    }

    for (Map.Entry<String, Edge> e : gdlHandler.getEdgeCache().entrySet()) {
      updateEdgeCache(e.getKey(), e.getValue());
    }
  }

  /**
   * Creates a new EPGMGraph from the GDL Loader.
   *
   * @param g graph from GDL Loader
   * @return EPGM graph head
   */
  private G initGraphHead(Graph g) {
    G graphHead = (G) config.getGraphHeadFactory().createGraphHead(
      g.getLabel(), Properties.createFromMap(g.getProperties()));
    graphHeadIds.put(g.getId(), graphHead.getId());
    graphHeads.put(graphHead.getId(), graphHead);
    return graphHead;
  }

  /**
   * Creates a new EPGMVertex from the GDL Loader or updates an existing one.
   *
   * @param v vertex from GDL Loader
   * @return EPGM vertex
   */
  private V initVertex(Vertex v) {
    V vertex;
    if (!vertexIds.containsKey(v.getId())) {
      vertex = (V) config.getVertexFactory().createVertex(
        v.getLabel(),
        Properties.createFromMap(v.getProperties()),
        createGradoopIdSet(v));
      vertexIds.put(v.getId(), vertex.getId());
      vertices.put(vertex.getId(), vertex);
    } else {
      vertex = vertices.get(vertexIds.get(v.getId()));
      vertex.setGraphIds(createGradoopIdSet(v));
    }
    return vertex;
  }

  /**
   * Creates a new EPGMEdge from the GDL Loader.
   *
   * @param e edge from GDL loader
   * @return EPGM edge
   */
  private E initEdge(Edge e) {
    E edge;
    if (!edgeIds.containsKey(e.getId())) {
      edge = (E) config.getEdgeFactory().createEdge(
        e.getLabel(),
        vertexIds.get(e.getSourceVertexId()),
        vertexIds.get(e.getTargetVertexId()),
        Properties.createFromMap(e.getProperties()),
        createGradoopIdSet(e));
      edgeIds.put(e.getId(), edge.getId());
      edges.put(edge.getId(), edge);
    } else {
      edge = edges.get(edgeIds.get(e.getId()));
      edge.setGraphIds(createGradoopIdSet(e));
    }
    return edge;
  }

  /**
   * Updates the graph cache.
   *
   * @param variable graph variable used in GDL script
   * @param g graph from GDL loader
   */
  private void updateGraphCache(String variable, Graph g) {
    graphHeadCache.put(
      variable, graphHeads.get(graphHeadIds.get(g.getId())));
  }

  /**
   * Updates the vertex cache.
   *
   * @param variable vertex variable used in GDL script
   * @param v vertex from GDL loader
   */
  private void updateVertexCache(String variable, Vertex v) {
    vertexCache.put(variable, vertices.get(vertexIds.get(v.getId())));
  }

  /**
   * Updates the edge cache.
   *
   * @param variable edge variable used in the GDL script
   * @param e edge from GDL loader
   */
  private void updateEdgeCache(String variable, Edge e) {
    edgeCache.put(variable, edges.get(edgeIds.get(e.getId())));
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
      result.add(graphHeadIds.get(graphId));
    }
    return result;
  }
}
