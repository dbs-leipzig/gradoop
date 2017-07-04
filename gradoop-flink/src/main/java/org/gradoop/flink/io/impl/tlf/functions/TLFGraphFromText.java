/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
package org.gradoop.flink.io.impl.tlf.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.gradoop.flink.io.impl.tlf.tuples.TLFEdge;
import org.gradoop.flink.io.impl.tlf.tuples.TLFVertex;
import org.gradoop.flink.io.impl.tlf.tuples.TLFGraph;
import org.gradoop.flink.io.impl.tlf.tuples.TLFGraphHead;

import java.util.Collection;
import java.util.List;

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
    List<TLFVertex> vertices = Lists.newArrayList();
    Collection<TLFEdge> edges = Lists.newArrayList();

    do {
      currChar = graph.charAt(cursor);
      if (currChar == '\n') {
        String[] fields = stringBuilder.toString().trim().split(" ");
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
