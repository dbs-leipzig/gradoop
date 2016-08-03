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

package org.gradoop.flink.util;

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.util.AsciiGraphLoader;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.EPGMDatabase;
import org.gradoop.flink.model.impl.GraphCollection;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

/**
 * Used the {@link AsciiGraphLoader} to generate instances of
 * {@link LogicalGraph} and {@link GraphCollection} from GDL.
 *
 * @see <a href="https://github.com/s1ck/gdl">GDL on GitHub</a>
 */
public class FlinkAsciiGraphLoader {

  /**
   * Gradoop Flink configuration
   */
  private final GradoopFlinkConfig config;

  /**
   * AsciiGraphLoader to create graph, vertex and edge collections.
   */
  private AsciiGraphLoader<GraphHead, Vertex, Edge> loader;

  /**
   * Creates a new FlinkAsciiGraphLoader instance.
   *
   * @param config Gradoop Flink configuration
   */
  public FlinkAsciiGraphLoader(GradoopFlinkConfig config) {
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
   * Initializes the database from the given ASCII GDL stream.
   *
   * @param stream GDL stream
   * @throws IOException
   */
  public void initDatabaseFromStream(InputStream stream) throws IOException {
    if (stream == null) {
      throw new IllegalArgumentException("AsciiGraph must not be null");
    }
    loader = AsciiGraphLoader.fromStream(stream, config);
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
  public LogicalGraph getLogicalGraphByVariable(String variable) {
    GraphHead graphHead = loader.getGraphHeadByVariable(variable);
    Collection<Vertex> vertices = loader.getVerticesByGraphVariables(variable);
    Collection<Edge> edges = loader.getEdgesByGraphVariables(variable);

    return LogicalGraph.fromCollections(graphHead, vertices, edges, config);
  }

  /**
   * Builds a {@link GraphCollection} from the graph referenced by the given
   * graph variables.
   *
   * @param variables graph variables used in GDL script
   * @return GraphCollection
   */
  public GraphCollection getGraphCollectionByVariables(String... variables) {
    Collection<GraphHead> graphHeads =
      loader.getGraphHeadsByVariables(variables);
    Collection<Vertex> vertices =
      loader.getVerticesByGraphVariables(variables);
    Collection<Edge> edges =
      loader.getEdgesByGraphVariables(variables);

    return GraphCollection.fromCollections(graphHeads, vertices, edges, config);
  }

  /**
   * Returns all GraphHeads contained in the ASCII graph.
   *
   * @return graphHeads
   */
  public Collection<GraphHead> getGraphHeads() {
    return loader.getGraphHeads();
  }

  /**
   * Returns EPGMGraphHead by given variable.
   *
   * @param variable variable used in GDL script
   * @return graphHead or {@code null} if graph is not cached
   */
  public GraphHead getGraphHeadByVariable(String variable) {
    return loader.getGraphHeadByVariable(variable);
  }

  /**
   * Returns all vertices contained in the ASCII graph.
   *
   * @return vertices
   */
  public Collection<Vertex> getVertices() {
    return loader.getVertices();
  }

  /**
   * Returns all vertices that belong to the given graph variables.
   *
   * @param variables graph variables used in the GDL script
   * @return vertices that are contained in the graphs
   */
  public Collection<Vertex> getVerticesByGraphVariables(String... variables) {
    return loader.getVerticesByGraphVariables(variables);
  }

  /**
   * Returns the vertex which is identified by the given variable. If the
   * variable cannot be found, the method returns {@code null}.
   *
   * @param variable vertex variable
   * @return vertex or {@code null} if variable is not used
   */
  public Vertex getVertexByVariable(String variable) {
    return loader.getVertexByVariable(variable);
  }

  /**
   * Returns all edges contained in the ASCII graph.
   *
   * @return edges
   */
  public Collection<Edge> getEdges() {
    return loader.getEdges();
  }

  /**
   * Returns all edges that belong to the given graph variables.
   *
   * @param variables graph variables used in the GDL script
   * @return edges
   */
  public Collection<Edge> getEdgesByGraphVariables(String... variables) {
    return loader.getEdgesByGraphVariables(variables);
  }

  /**
   * Returns the edge which is identified by the given variable. If the
   * variable cannot be found, the method returns {@code null}.
   *
   * @param variable edge variable
   * @return edge or {@code null} if variable is not used
   */
  public Edge getEdgeByVariable(String variable) {
    return loader.getEdgeByVariable(variable);
  }

  /**
   * Returns the complete database represented by the loader.
   *
   * @return EPGM database
   */
  @SuppressWarnings("unchecked")
  public EPGMDatabase getDatabase() {
    return EPGMDatabase
      .fromCollections(loader.getGraphHeads(),
        loader.getVertices(),
        loader.getEdges(),
        config);
  }
}
