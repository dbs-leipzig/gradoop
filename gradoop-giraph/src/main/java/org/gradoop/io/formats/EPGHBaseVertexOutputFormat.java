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

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.gradoop.storage.hbase.VertexHandler;

import java.io.IOException;
import java.util.List;

/**
 * Used to write the result of a graph computation into HBase. The graph is
 * based on the EPG model.
 */
public class EPGHBaseVertexOutputFormat extends
  HBaseVertexOutputFormat<EPGVertexIdentifierWritable,
    EPGVertexValueWritable, EPGEdgeValueWritable> {

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexWriter<EPGVertexIdentifierWritable, EPGVertexValueWritable,
    EPGEdgeValueWritable> createVertexWriter(
    TaskAttemptContext context) throws IOException, InterruptedException {
    return new EPGHBaseVertexWriter(context);
  }

  /**
   * Writes a single giraph vertex back to HBase.
   */
  public static class EPGHBaseVertexWriter extends
    HBaseVertexWriter<EPGVertexIdentifierWritable, EPGVertexValueWritable,
      EPGEdgeValueWritable> {

    /**
     * Sets up base table output format and creates a record writer.
     *
     * @param context task attempt context
     */
    public EPGHBaseVertexWriter(TaskAttemptContext context) throws IOException,
      InterruptedException {
      super(context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeVertex(
      Vertex<EPGVertexIdentifierWritable, EPGVertexValueWritable,
        EPGEdgeValueWritable> vertex) throws
      IOException, InterruptedException {
      RecordWriter<ImmutableBytesWritable, Mutation> writer = getRecordWriter();
      VertexHandler vertexHandler = getVertexHandler();
      // vertex identifier
      byte[] rowKey = vertexHandler.getRowKey(vertex.getId().getId());
      Put put = new Put(rowKey);
      // labels
      put = vertexHandler.writeLabel(put, vertex.getValue());
      // properties
      put = vertexHandler.writeProperties(put, vertex.getValue());
      // graphs
      put = vertexHandler.writeGraphs(put, vertex.getValue());
      // outgoing edges
      List<org.gradoop.model.Edge> edges =
        Lists.newArrayListWithCapacity(vertex.getNumEdges());
      for (Edge<EPGVertexIdentifierWritable, EPGEdgeValueWritable> edge : vertex
        .getEdges()) {
        edges.add(edge.getValue());
      }
      vertexHandler.writeOutgoingEdges(put, edges);
      writer.write(new ImmutableBytesWritable(rowKey), put);
    }
  }
}
