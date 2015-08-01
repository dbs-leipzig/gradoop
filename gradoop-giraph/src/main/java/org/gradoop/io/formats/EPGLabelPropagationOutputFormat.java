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

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.gradoop.io.LabelPropagationValue;
import org.gradoop.model.impl.VertexFactory;
import org.gradoop.storage.hbase.VertexHandler;

import java.io.IOException;

/**
 * Used to write resulting vertices of BTG Computation to HBase.
 */
public class EPGLabelPropagationOutputFormat extends
  HBaseVertexOutputFormat<LongWritable, LabelPropagationValue, NullWritable> {
  /**
   * {@inheritDoc}
   */
  @Override
  public VertexWriter<LongWritable, LabelPropagationValue, NullWritable>
  createVertexWriter(
    TaskAttemptContext context) throws IOException, InterruptedException {
    return new LPHBaseVertexWriter(context);
  }

  /**
   * Writes a single Giraph vertex back to HBase.
   */
  public static class LPHBaseVertexWriter extends
    HBaseVertexWriter<LongWritable, LabelPropagationValue, NullWritable> {
    /**
     * Sets up HBase table output format and creates a record writer.
     *
     * @param context task attempt context
     */
    public LPHBaseVertexWriter(TaskAttemptContext context) throws IOException,
      InterruptedException {
      super(context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeVertex(
      Vertex<LongWritable, LabelPropagationValue, NullWritable> vertex) throws
      IOException, InterruptedException {
      RecordWriter<ImmutableBytesWritable, Mutation> writer = getRecordWriter();
      VertexHandler vertexHandler = getVertexHandler();
      byte[] rowKey = vertexHandler.getRowKey(vertex.getId().get());
      Put put = new Put(rowKey);
      // just need to write the Values
      org.gradoop.model.Vertex v =
        VertexFactory.createDefaultVertexWithID(vertex.getId().get());
      v.addGraph(vertex.getValue().getCurrentCommunity().get());
      put = vertexHandler.writeGraphs(put, v);
      writer.write(new ImmutableBytesWritable(rowKey), put);
    }
  }
}
