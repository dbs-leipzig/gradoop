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
    String[] lines = inputTuple.getField(1).toString().split("\n");

    boolean firstLine = true;

    TLFGraphHead graphHead = null;
    Collection<TLFVertex> vertices = Lists.newArrayList();
    Collection<TLFEdge> edges = Lists.newArrayList();

    for (String line :lines) {
      if (firstLine) {
        graphHead = new TLFGraphHead(inputTuple.f0.get());
        firstLine = false;
      } else {
        String[] fields = line.split(" ");

        if (fields[0].equals(TLFVertex.SYMBOL)) {
          vertices.add(new TLFVertex(
            Integer.valueOf(fields[1]),
            fields[2])
          );
        } else {
          edges.add(new TLFEdge(
            Integer.valueOf(fields[1]),
            Integer.valueOf(fields[2]),
            fields[3]
          ));
        }
      }
    }

    return new TLFGraph(graphHead, vertices, edges);
  }
}
