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

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Encodes the output of the {@link org.gradoop.biiig.algorithms.BTGComputation} in
 * the following format: vertex-id,vertex-class vertex-value[ btg-id]* e.g. the
 * following line 0,0 3.14 23 42 decodes vertex-id 0 with vertex-class 0 (0 =
 * transactional, 1 = master) and the value 3.14. The node is connected to two
 * BTGs (23, 42).
 */
public class BTGTextVertexOutputFormat extends
  TextVertexOutputFormat<LongWritable, BTGVertexValue, NullWritable> {

  /**
   * Used for splitting the line into the main tokens (vertex id, vertex value,
   * edges)
   */
  private static final String LINE_TOKEN_SEPARATOR = ",";

  /**
   * Used for splitting a main token into its values (vertex value = type,
   * value, btg-ids; edge list)
   */
  private static final String VALUE_TOKEN_SEPARATOR = " ";

  /**
   * @param context the information about the task
   * @return the text vertex writer to be used
   * @throws java.io.IOException
   * @throws InterruptedException
   */
  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    return new BTGTextVertexLineWriter();
  }

  /**
   * Used to convert a {@link BTGVertexValue} to a line in the output file.
   */
  private class BTGTextVertexLineWriter extends TextVertexWriterToEachLine {

    /**
     * Writes a line for the given vertex.
     *
     * @param vertex the current vertex for writing
     * @return the text line to be written
     * @throws java.io.IOException exception that can be thrown while writing
     */
    @Override
    protected Text convertVertexToLine(
      Vertex<LongWritable, BTGVertexValue, NullWritable> vertex)
      throws IOException {
      StringBuilder sb = new StringBuilder();
      // vertex-id
      sb.append(vertex.getId());
      sb.append(LINE_TOKEN_SEPARATOR);
      // vertex-value (=vertex-class, vertex value and btg-ids)
      sb.append(vertex.getValue().getVertexType().ordinal());
      sb.append(VALUE_TOKEN_SEPARATOR);
      sb.append(vertex.getValue().getVertexValue());
      for (Long btgID : vertex.getValue().getGraphs()) {
        sb.append(VALUE_TOKEN_SEPARATOR);
        sb.append(btgID);
      }
      return new Text(sb.toString());
    }
  }
}
