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

package org.gradoop.flink.model.impl.nested.datastructures;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.nested.datastructures.equality.NormalizedGraphEquality;
import org.gradoop.flink.model.impl.nested.datastructures.functions.MapGraphHeadAsVertex;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToDataString;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * The normalization process consists of creating a new Graph containing a vertex per graph
 * head. This is required by the new nested graph model, where to each graph head corresponds
 * a vertex.
 */
public class NormalizedGraph {

  /**
   * Normalized heads
   */
  private DataSet<GraphHead> heads;

  /**
   * Normalized vertices
   */
  private DataSet<Vertex> vertices;

  /**
   * Normalized edges
   */
  private DataSet<Edge> edges;

  /**
   * Default configuration
   */
  private GradoopFlinkConfig conf;

  /**
   * Creates a normalized graph from a graph collection.
   * @param lg    Graph Collection to be normalized
   */
  public NormalizedGraph(GraphCollection lg) {
    this(lg.getGraphHeads(), lg.getVertices(), lg.getEdges(), lg.getConfig());
  }

  /**
   * Creates a normalized graph from a logical graph.
   * @param lg  Logical Graph to be normalized
   */
  public NormalizedGraph(LogicalGraph lg) {
    this(lg.getGraphHead(), lg.getVertices(), lg.getEdges(), lg.getConfig());
  }

  /**
   * NormalizedGraph constructor from raw elements
   * @param heads     GraphHeads
   * @param vertices  Graph vertices
   * @param edges     Graph edges
   * @param conf      Default configuration
   */
  NormalizedGraph(DataSet<GraphHead> heads, DataSet<Vertex> vertices, DataSet<Edge> edges,
    GradoopFlinkConfig conf) {
    this.vertices = heads
      .map(new MapGraphHeadAsVertex())
      .union(vertices)
      .distinct(new Id<>());
    this.heads = heads;
    this.edges = edges;
    this.conf = conf;
  }

  /**
   * Returns…
   * @return  the graph head
   */
  public DataSet<GraphHead> getGraphHeads() {
    return heads;
  }

  /**
   * Returns…
   * @return  the vertices
   */
  public DataSet<Vertex> getVertices() {
    return vertices;
  }

  /**
   * Returs…
   * @return  the edges
   */
  public DataSet<Edge> getEdges() {
    return edges;
  }

  /**
   * Returns…
   * @return  the gradoop configuration
   */
  public GradoopFlinkConfig getConfig() {
    return conf;
  }

  /**
   * Adds some new edges to the dataset
   * @param edges Edges to be added through union
   */
  public void updateEdgesWithUnion(DataSet<Edge> edges) {
    this.edges = this.edges.union(edges);
  }

  /**
   * Checks if the two NormalizedGraph are similar by data
   * @param research  Element to which the similarity has to be compared
   * @return          A dataset containing the result
   */
  public DataSet<Boolean> equalsByData(NormalizedGraph research) {
    return new NormalizedGraphEquality(
        new GraphHeadToDataString(),
        new VertexToDataString(),
        new EdgeToDataString(), true).execute(this, research);
  }

  /**
   * Adds some new vertices to the dataset
   * @param map Vertices to be added through union
   */
  public void updateVerticesWithUnion(DataSet<Vertex> map) {
    this.vertices = this.vertices.union(map);
  }
}
