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

package org.gradoop.util;

import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.EPGMDatabase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;

import java.io.IOException;
import java.util.Collection;

/**
 * Used the {@link AsciiGraphLoader} to generate instances of
 * {@link LogicalGraph} and {@link GraphCollection} from GDL.
 *
 * @param <G> EPGM graph type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 *
 * @see <a href="https://github.com/s1ck/gdl">GDL on GitHub</a>
 */
public class FlinkAsciiGraphLoader<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge> {

  /**
   * Gradoop Flink configuration
   */
  private final GradoopFlinkConfig<G, V, E> config;

  /**
   * AsciiGraphLoader to create graph, vertex and edge collections.
   */
  private AsciiGraphLoader<G, V, E> loader;

  /**
   * Creates a new FlinkAsciiGraphLoader instance.
   *
   * @param config Gradoop Flink configuration
   */
  public FlinkAsciiGraphLoader(GradoopFlinkConfig<G, V, E> config) {
    if (config == null) {
      throw new IllegalArgumentException("Config must not be null.");
    }
    this.config = config;
  }

  /**
   * Initializes the database from the given ASCII GDL string.
   *
   * @param asciiGraphs GDL string (must not be {@code null})
   */
  public void initDatabaseFromString(String asciiGraphs) {
    if (asciiGraphs == null) {
      throw new IllegalArgumentException("AsciiGraph must not be null");
    }
    loader = AsciiGraphLoader.fromString(asciiGraphs, config);
  }

  /**
   * Appends the given ASCII GDL String to the database.
   *
   * Variables previously used can be reused as their refer to the same objects.
   *
   * @param asciiGraph GDL string (must not be {@code null})
   */
  public void appendToDatabaseFromString(String asciiGraph) {
    if (asciiGraph == null) {
      throw new IllegalArgumentException("AsciiGraph must not be null");
    }
    if (loader != null) {
      loader.appendFromString(asciiGraph);
    } else {
      initDatabaseFromString(asciiGraph);
    }
  }

  /**
   * Initializes the database from the given GDL file.
   *
   * @param fileName GDL file name (must not be {@code null})
   * @throws IOException
   */
  public void initDatabaseFromFile(String fileName) throws IOException {
    if (fileName == null) {
      throw new IllegalArgumentException("FileName must not be null.");
    }
    loader = AsciiGraphLoader.fromFile(fileName, config);
  }

  /**
   * Builds a {@link LogicalGraph} from the graph referenced by the given
   * graph variable.
   *
   * @param variable graph variable used in GDL script
   * @return LogicalGraph
   */
  public LogicalGraph<G, V, E> getLogicalGraphByVariable(String variable) {
    G graphHead = loader.getGraphHeadByVariable(variable);
    Collection<V> vertices = loader.getVerticesByGraphVariables(variable);
    Collection<E> edges = loader.getEdgesByGraphVariables(variable);

    return LogicalGraph.fromCollections(graphHead, vertices, edges, config);
  }

  /**
   * Builds a {@link GraphCollection} from the graph referenced by the given
   * graph variables.
   *
   * @param variables graph variables used in GDL script
   * @return GraphCollection
   */
  public GraphCollection<G, V, E> getGraphCollectionByVariables(
    String... variables) {
    Collection<G> graphHeads = loader.getGraphHeadsByVariables(variables);
    Collection<V> vertices = loader.getVerticesByGraphVariables(variables);
    Collection<E> edges = loader.getEdgesByGraphVariables(variables);

    return GraphCollection.fromCollections(graphHeads, vertices, edges, config);
  }

  /**
   * Returns all GraphHeads contained in the ASCII graph.
   *
   * @return graphHeads
   */
  public Collection<G> getGraphHeads() {
    return loader.getGraphHeads();
  }

  /**
   * Returns GraphHead by given variable.
   *
   * @param variable variable used in GDL script
   * @return graphHead or {@code null} if graph is not cached
   */
  public G getGraphHeadByVariable(String variable) {
    return loader.getGraphHeadByVariable(variable);
  }

  /**
   * Returns all vertices contained in the ASCII graph.
   *
   * @return vertices
   */
  public Collection<V> getVertices() {
    return loader.getVertices();
  }

  /**
   * Returns all vertices that belong to the given graph variables.
   *
   * @param variables graph variables used in the GDL script
   * @return vertices that are contained in the graphs
   */
  public Collection<V> getVerticesByGraphVariables(String... variables) {
    return loader.getVerticesByGraphVariables(variables);
  }

  /**
   * Returns the vertex which is identified by the given variable. If the
   * variable cannot be found, the method returns {@code null}.
   *
   * @param variable vertex variable
   * @return vertex or {@code null} if variable is not used
   */
  public V getVertexByVariable(String variable) {
    return loader.getVertexByVariable(variable);
  }

  /**
   * Returns all edges contained in the ASCII graph.
   *
   * @return edges
   */
  public Collection<E> getEdges() {
    return loader.getEdges();
  }

  /**
   * Returns all edges that belong to the given graph variables.
   *
   * @param variables graph variables used in the GDL script
   * @return edges
   */
  public Collection<E> getEdgesByGraphVariables(String... variables) {
    return loader.getEdgesByGraphVariables(variables);
  }

  /**
   * Returns the edge which is identified by the given variable. If the
   * variable cannot be found, the method returns {@code null}.
   *
   * @param variable edge variable
   * @return edge or {@code null} if variable is not used
   */
  public E getEdgeByVariable(String variable) {
    return loader.getEdgeByVariable(variable);
  }

  /**
   * Returns the complete database represented by the loader.
   *
   * @return EPGM database
   */
  @SuppressWarnings("unchecked")
  public EPGMDatabase<G, V, E> getDatabase() {
    return EPGMDatabase
      .fromCollection(loader.getGraphHeads(),
        loader.getVertices(),
        loader.getEdges(),
        config);
  }
}
