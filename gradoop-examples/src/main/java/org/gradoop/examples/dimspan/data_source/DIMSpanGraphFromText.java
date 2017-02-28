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

package org.gradoop.examples.dimspan.data_source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.LabeledGraphStringString;

/**
 * Turns a TLF-formatted graph into a DIMSpan string-labeled graph.
 */
public class DIMSpanGraphFromText
  implements MapFunction<Tuple2<LongWritable, Text>, LabeledGraphStringString> {

  /**
   * Line break character
   */
  public static final char LINE_BREAK = '\n';

  /**
   * Columne separator
   */
  public static final char COLUMN_SEPARATOR = ' ';

  @Override
  public LabeledGraphStringString map(Tuple2<LongWritable, Text> inputTuple) throws Exception {
    LabeledGraphStringString setPair = LabeledGraphStringString.getEmptyOne();

    // create character array
    char[] chars = inputTuple.f1.toString().toCharArray();

    int i = 0;

    // read graph
    while (i < chars.length && chars[i] != LINE_BREAK) {
      i++;
    }
    i++;

    // read vertices
    while (i < chars.length && chars[i] == 'v') {
      i = readVertex(chars, i, setPair);
      i++;
    }

    // read edges
    while (i < chars.length) {
      i = readEdge(chars, i, setPair);
      i++;
    }

    return setPair;
  }


  /**
   * Reads a vertex.
   *
   * @param chars character array
   * @param i vertex start offset
   * @param graph output graph
   * @return next element's offset
   */
  private int readVertex(char[] chars, int i, LabeledGraphStringString graph) {
    StringBuilder labelBuilder = new StringBuilder(1);

    char c;

    do {
      i++;
      c = chars[i];
    } while (c != COLUMN_SEPARATOR);

    // read vertex id
    i++;
    c = chars[i];
    do {
      i++;
      c = chars[i];
    } while (c != COLUMN_SEPARATOR);

    // read vertex label
    i++;
    c = chars[i];
    do {
      labelBuilder.append(c);
      i++;
      c = chars[i];
    } while (c != LINE_BREAK);

    graph.addVertex(labelBuilder.toString());

    return i;
  }

  /**
   * Reads an edge.
   *
   * @param chars character array
   * @param i edge start offset
   * @param graph output graph
   * @return next element's offset
   */
  private int readEdge(char[] chars, int i, LabeledGraphStringString graph) {
    StringBuilder sourceBuilder = new StringBuilder(2);
    StringBuilder targetBuilder = new StringBuilder(2);
    StringBuilder labelBuilder = new StringBuilder(1);

    char c;

    do {
      i++;
      c = chars[i];
    } while (c != COLUMN_SEPARATOR);

    // read source id
    i++;
    c = chars[i];
    do {
      sourceBuilder.append(c);
      i++;
      c = chars[i];
    } while (c != COLUMN_SEPARATOR);

    // read target id
    i++;
    c = chars[i];
    do {
      targetBuilder.append(c);
      i++;
      c = chars[i];
    } while (c != COLUMN_SEPARATOR);

    // read edge label
    i++;
    c = chars[i];
    do {
      labelBuilder.append(c);

      i++;

      if (i < chars.length) {
        c = chars[i];
      } else {
        break;
      }
    } while (c != LINE_BREAK);

    int sourceId = Integer.parseInt(sourceBuilder.toString());
    int targetId = Integer.parseInt(targetBuilder.toString());
    String label = labelBuilder.toString();

    graph.addEdge(sourceId, label, targetId);

    return i;
  }
}
