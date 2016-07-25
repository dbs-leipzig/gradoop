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

import com.google.common.collect.Lists;
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
    boolean firstLine = true;
    boolean vertexLine = true;
    String graph = inputTuple.f1.toString();
    StringBuilder stringBuilder = new StringBuilder();
    int cursor = 0;
    char currChar;

    TLFGraphHead graphHead = null;
    Collection<TLFVertex> vertices = Lists.newArrayList();
    Collection<TLFEdge> edges = Lists.newArrayList();

    do {
      currChar = graph.charAt(cursor);
      if (currChar == '\n') {
        String[] fields = stringBuilder.toString().split(" ");
        if (firstLine) {
          graphHead = new TLFGraphHead(Long.valueOf(fields[2]));
          firstLine = false;
        } else {
          if (vertexLine) {
            vertices.add(new TLFVertex(
              Integer.valueOf(fields[1]),
              fields[2])
            );
            if (TLFEdge.SYMBOL.equals(String.valueOf(graph
              .charAt(cursor + 1)))) {
              vertexLine = false;
            }
          } else {
            edges.add(new TLFEdge(
              Integer.valueOf(fields[1]),
              Integer.valueOf(fields[2]),
              fields[3]
            ));
          }
        }
        stringBuilder.setLength(0);
      } else {
        stringBuilder.append(currChar);
      }
      cursor++;
    } while (cursor != graph.length());

    return new TLFGraph(graphHead, vertices, edges);
  }
}
