/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gradoop.biiig.io.formats;

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * An IIG vertex is decoded in the following format: vertex-id,vertex-class
 * vertex-value[ btg-id]*,[neighbour-vertex-id ]* e.g. the following line 0,0
 * 3.14 4 9,1 2 decodes vertex-id 0 with vertex-class 0 (0 = transactional, 1 =
 * master) and value 3.14. The node is connected to two BTGs (4,9) and has edges
 * to two vertices (1,2).
 */
public class BTGTextVertexInputFormat extends
  TextVertexInputFormat<LongWritable, BTGVertexValue, NullWritable> {

  /**
   * Used for splitting the line into the main tokens (vertex id, vertex value,
   * edges)
   */
  private static final Pattern LINE_TOKEN_SEPARATOR = Pattern.compile("[,]");

  /**
   * Used for splitting a main token into its values (vertex value = type,
   * value, btg-ids; edge list)
   */
  private static final Pattern VALUE_TOKEN_SEPARATOR = Pattern.compile("[ ]");

  /**
   * @param split   the split to be read
   * @param context the information about the task
   * @return the text vertex reader to be used
   * @throws java.io.IOException
   */
  @Override
  public TextVertexReader createVertexReader(InputSplit split,
                                             TaskAttemptContext context)
    throws
    IOException {
    return new IIGTextVertexReaderFromEachLine();
  }

  /**
   * Used to convert a line in the input file to a {@link org.apache.giraph
   * .examples.biiig.io.IIGVertex}.
   */
  private class IIGTextVertexReaderFromEachLine extends
    TextVertexReaderFromEachLineProcessed<String[]> {

    @Override
    protected String[] preprocessLine(Text tokens)
      throws IOException {
      return LINE_TOKEN_SEPARATOR.split(tokens.toString());
    }

    @Override
    protected LongWritable getId(String[] tokens)
      throws IOException {
      return new LongWritable(Long.parseLong(tokens[0]));
    }

    @Override
    protected BTGVertexValue getValue(String[] tokens)
      throws IOException {
      String[] valueTokens = VALUE_TOKEN_SEPARATOR.split(tokens[1]);
      BTGVertexType vertexClass = BTGVertexType.values()[Integer.parseInt(
        valueTokens[0])];
      Double vertexValue = Double.parseDouble(valueTokens[1]);
      List<Long> btgIDs = Lists.newArrayListWithCapacity(
        valueTokens.length - 1);
      for (int n = 2; n < valueTokens.length; n++) {
        btgIDs.add(Long.parseLong(valueTokens[n]));
      }
      return new BTGVertexValue(vertexClass, vertexValue, btgIDs);
    }

    @Override
    protected Iterable<Edge<LongWritable, NullWritable>> getEdges(
      String[] tokens)
      throws IOException {
      String[] edgeTokens = (tokens.length == 3) ? VALUE_TOKEN_SEPARATOR.split(
        tokens[2]) : new String[0];
      List<Edge<LongWritable, NullWritable>> edges =
        Lists.newArrayListWithCapacity(edgeTokens.length);
      for (String edgeToken : edgeTokens) {
        edges.add(EdgeFactory.create(
          new LongWritable(Long.parseLong(edgeToken))));
      }
      return edges;
    }
  }
}
