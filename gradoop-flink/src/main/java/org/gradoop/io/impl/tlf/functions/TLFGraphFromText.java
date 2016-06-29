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

package org.gradoop.io.impl.tlf.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.gradoop.io.impl.tlf.tuples.TLFEdge;
import org.gradoop.io.impl.tlf.tuples.TLFGraph;
import org.gradoop.io.impl.tlf.tuples.TLFGraphHead;
import org.gradoop.io.impl.tlf.tuples.TLFVertex;

import java.util.Collection;

/**
 * Reads graph imported from a TLF file. The result of the mapping is a
 * dataset of of tlf graphs, with each TLFGraph consisting of a tlf graph
 * head, a collection of tlf vertices and a collection of tlf edges.
 */
public class TLFGraphFromText
  implements MapFunction<Tuple2<LongWritable, Text>, TLFGraph> {

  /**
   * Cunstructs a dataset containing TLFGraph(s).
   *
   * @param inputTuple consists of a key(LongWritable) and a value(Text)
   * @return a TLFGraph created by the input text
   * @throws Exception
   */
  @Override
  public TLFGraph map(Tuple2<LongWritable, Text> inputTuple) throws Exception {
    String graphString = inputTuple.getField(1).toString();

    TLFGraphHead graphHead;
    Collection<TLFVertex> vertices = Sets.newHashSet();
    Collection<TLFEdge> edges = Sets.newHashSet();

    graphHead = getGraphHead(graphString);
    vertices.addAll(getVertices(graphString));
    edges.addAll(getEdges(graphString));

    return new TLFGraph(graphHead, vertices, edges);
  }

  /**
   * Returns the graph head defined in content.
   *
   * @param content containing current graph as string
   * @return returns the graph head defined in content
   */
  private TLFGraphHead getGraphHead(String content) {
    //Position of id which is between 't # ' and the end of the line
    return new TLFGraphHead(Long.parseLong(content.substring(content.indexOf
      ("t # ") + 4, content.indexOf("\n")).trim()));
  }

  /**
   * Reads the vertices of a complete TLF graph segment.
   *
   * @param content the TLF graph segment
   * @return collection of vertices as tuples
   */
  private Collection<TLFVertex> getVertices(String content) {
    Collection<TLFVertex> vertexCollection = Sets.newHashSet();
    String[] vertex;
    //-1 cause before e is \n
    content = content.substring(content.indexOf("v"), content.indexOf("e") - 1);
    String[] vertices = content.split("\n");

    for (int i = 0; i < vertices.length; i++) {
      vertex = vertices[i].split(" ");
      vertexCollection.add(new TLFVertex(Integer.parseInt(
        vertex[1].trim()), vertex[2].trim()));
    }
    return vertexCollection;
  }

  /**
   * Reads the edges of a complete TLF graph segment.
   *
   * @param content the TLF graph segment
   * @return collection of edges as tuples
   */
  private Collection<TLFEdge> getEdges(String content) {
    Collection<TLFEdge> edgeCollection = Sets.newHashSet();
    String[] edge;

    content = content.substring(content.indexOf("e"), content.length());
    String[] edges = content.split("\n");

    for (int i = 0; i < edges.length; i++) {
      edge = edges[i].split(" ");
      edgeCollection.add(new TLFEdge(Integer
        .parseInt(edge[1].trim()), Integer.parseInt(edge[2].trim()), edge[3]));
    }
    return edgeCollection;
  }
}
