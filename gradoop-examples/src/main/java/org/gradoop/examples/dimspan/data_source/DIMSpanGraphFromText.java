/*
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
