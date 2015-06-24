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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.io.formats;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Encodes the output of the {@link org.gradoop.algorithms
 * .AdaptiveRepartitioningComputation} in
 * the following format:
 * <p/>
 * {@code <vertex-id> <partition-id> \[<partition-id>*\] [<neighbour-id>]*}
 */
public class TextLabelPropagationOutputFormat extends
  TextVertexOutputFormat<LongWritable, LongWritable, NullWritable> {

  /**
   * Used for splitting the line into the main tokens (vertex id, vertex value
   */
  private static final String VALUE_TOKEN_SEPARATOR = " ";

  /**
   * @param context the information about the task
   * @return the text vertex writer to be used
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws
    IOException, InterruptedException {
    return new LabelPropagationVertexLineWriter();
  }

  /**
   * Used to convert a {@link org.gradoop.io.PartitioningVertex} to a
   * line in the output file.
   */
  private class LabelPropagationVertexLineWriter extends
    TextVertexWriterToEachLine {
    /**
     * {@inheritDoc}
     */
    @Override
    protected Text convertVertexToLine(
      Vertex<LongWritable, LongWritable, NullWritable> vertex) throws
      IOException {
      // vertex id
      StringBuilder sb = new StringBuilder(vertex.getId().toString());
      sb.append(VALUE_TOKEN_SEPARATOR);
      // vertex value
      sb.append(vertex.getValue().toString());
      sb.append(VALUE_TOKEN_SEPARATOR);
      // edges
      for (Edge<LongWritable, NullWritable> e : vertex.getEdges()) {
        sb.append(e.getTargetVertexId().toString());
        sb.append(VALUE_TOKEN_SEPARATOR);
      }
      return new Text(sb.toString());
    }
  }
}
