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

package org.gradoop.io.graphgen;

import org.gradoop.io.graphgen.tuples.GraphGenEdge;
import org.gradoop.io.graphgen.tuples.GraphGenGraph;
import org.gradoop.io.graphgen.tuples.GraphGenGraphHead;
import org.gradoop.io.graphgen.tuples.GraphGenVertex;

import java.util.Collection;
import java.util.HashSet;

/**
 * Generates a collection of 3-tuples which contain the graph head, the
 * vertices and the edges from a GraphGen string
 */
public class GraphGenStringToCollection {

  /**
   * The 3-tuple collection containing the graph head, the vertices and edges
   */
  protected Collection<GraphGenGraph> graphCollection;

  /**
   * GraphGen string representation of the graph
   */
  protected String content;

  /**
   * Constructor which initiates a new collection for 3-tuples
   */
  public GraphGenStringToCollection() {
    graphCollection = new HashSet<>();
  }

  /**
   * Creates, or returns if already created, the collection of graphs as
   * tuples from content.
   *
   * @return Collection of graphs as tuples
   */
  public Collection<GraphGenGraph> getGraphCollection() {

    if (content.isEmpty()) {
      return null;
    }
    if (!graphCollection.isEmpty()) {
      return graphCollection;
    }
    GraphGenGraphHead graphHead;
    Collection<GraphGenVertex> vertices = new HashSet<>();
    Collection<GraphGenEdge> edges = new HashSet<>();

    //remove first tag so that split does not have empty first element
    String[] contentArray = content.trim().replaceFirst("t # ", "").split("t " +
      "# ");

    for (int i = 0; i < contentArray.length; i++) {
      vertices.addAll(this.getVertices(contentArray[i]));
      edges.addAll(this.getEdges(contentArray[i]));
      graphHead = this.getGraphHead(contentArray[i]);

      graphCollection.add(new GraphGenGraph(graphHead, vertices,
        edges));

      vertices = new HashSet<>();
      edges = new HashSet<>();
    }
    return graphCollection;
  }

  /**
   * Reads the vertices of a complete GraphGen graph segment.
   *
   * @param content the GraphGen graph segment
   * @return collection of vertices as tuples
   */
  protected Collection<GraphGenVertex> getVertices(String content) {
    Collection<GraphGenVertex> vertexCollection = new HashSet<>();
    String[] vertex;
    //-1 cause before e is \n
    content = content.substring(content.indexOf("v"), content.indexOf("e") - 1);
    String[] vertices = content.split("\n");

    for (int i = 0; i < vertices.length; i++) {
      vertex = vertices[i].split(" ");
      vertexCollection.add(new GraphGenVertex(Integer.parseInt(
        vertex[1].trim()), vertex[2].trim()));
    }
    return vertexCollection;
  }

  /**
   * Reads the edges of a complete GraphGen graph segment
   *
   * @param content the GraphGen graph segment
   * @return collection of edges as tuples
   */
  protected Collection<GraphGenEdge> getEdges(String
    content) {

    Collection<GraphGenEdge> edgeCollection = new
      HashSet<>();
    String[] edge;

    content = content.substring(content.indexOf("e"), content.length());
    String[] edges = content.split("\n");

    for (int i = 0; i < edges.length; i++) {
      edge = edges[i].split(" ");
      edgeCollection.add(new GraphGenEdge(Integer
        .parseInt(edge[1].trim()), Integer.parseInt(edge[2].trim()), edge[3]));
    }
    return edgeCollection;
  }

  /**
   * @param content containing current graph as string
   * @return returns the graph head defined in content
   */
  protected GraphGenGraphHead getGraphHead(String content) {
    return new GraphGenGraphHead(Long.parseLong(content.substring(0, 1)));
  }

  /**
   * Sets the current content.
   * @param content the content to be set.
   */
  public void setContent(String content) {
    this.content = content;
  }
}
