/**
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
package org.gradoop.flink.util;

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.util.AsciiGraphLoader;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.EPGMDatabase;

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

    return config.getLogicalGraphFactory().fromCollections(graphHead, vertices, edges);
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

    return config.getGraphCollectionFactory().fromCollections(graphHeads, vertices, edges);
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
   * Returns the graph heads assigned to the specified variables.
   *
   * @param variables variables used in the GDL script
   * @return graphHeads assigned to the variables
   */
  public Collection<GraphHead> getGraphHeadsByVariables(String... variables) {
    return loader.getGraphHeadsByVariables(variables);
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
