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

package org.gradoop.flink.io.reader.parsers;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Represents a to-be-created graph as a DatSet of vertices and edges.
 *
 * @param <Element> comparable element
 */
public class GraphClob<Element extends Comparable<Element>> {

  /**
   * Representation of the parsed vertices
   */
  private DataSet<ImportVertex<Element>> vertices;

  /**
   * Representation of the parsed edges
   */
  private DataSet<ImportEdge<Element>> edges;

  /**
   * Default environment
   */
  private GradoopFlinkConfig env;

  /**
   * Adds the first chunk of the parsed graph within the data structure
   * @param vertices    Vertex Set
   * @param edges       Edge Set
   */
  public GraphClob(DataSet<ImportVertex<Element>> vertices, DataSet<ImportEdge<Element>> edges) {
    this.vertices = vertices;
    this.edges = edges;
    env = GradoopFlinkConfig.createConfig(ExecutionEnvironment.getExecutionEnvironment());
  }

  /**
   * Changes the default configuration into a new one
   * @param conf  Uses a user-defined configuration
   */
  public void setGradoopFlinkConfiguration(GradoopFlinkConfig conf) {
    this.env = conf;
  }

  /**
   * Updates the current ParsableGraphClob with another one.
   * Merges the two to-be created graphs together
   * @param x To-be created graph
   * @return  The update instance of the current object
   */
  public GraphClob addAll(GraphClob<Element> x) {
    vertices = vertices.union(x.vertices);
    edges = edges.union(x.edges);
    return this;
  }

  /**
   * Updates the current GraphClob with another one.
   * Merges the two to-be created graphs together
   * @param v Vertex Set
   * @param e Edge Set
   * @return  The update instance of the current object
   */
  public GraphClob addAll(DataSet<ImportVertex<Element>> v, DataSet<ImportEdge<Element>> e) {
    vertices = vertices.union(v);
    edges = edges.union(e);
    return this;
  }

  /**
   * Transforms an external graph into an EPGM database.
   * @return  EPGM Database
   */
  public GraphDataSource<Element> asGraphDataSource() {
    return new GraphDataSource<>(vertices, edges, env);
  }

}
