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

package org.gradoop.biiig.io.formats;

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.gradoop.io.formats.HBaseVertexInputFormat;
import org.gradoop.storage.hbase.VertexHandler;

import java.io.IOException;
import java.util.List;

/**
 * Used to read vertices for BTG Computation from HBase.
 */
public class BTGHBaseVertexInputFormat extends
  HBaseVertexInputFormat<LongWritable, BTGVertexValue, NullWritable> {

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexReader<LongWritable, BTGVertexValue, NullWritable>
  createVertexReader(
    InputSplit split, TaskAttemptContext context) throws IOException {
    return new BTGHBaseVertexReader(split, context);
  }

  /**
   * Reads a single giraph vertex for BTG computation from a HBase row result.
   */
  public static class BTGHBaseVertexReader extends
    HBaseVertexReader<LongWritable, BTGVertexValue, NullWritable> {

    /**
     * Sets the base TableInputFormat and creates a record reader.
     *
     * @param split   InputSplit
     * @param context Context
     * @throws java.io.IOException
     */
    public BTGHBaseVertexReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
      super(split, context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      return getRecordReader().nextKeyValue();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Vertex<LongWritable, BTGVertexValue, NullWritable>
    getCurrentVertex() throws
      IOException, InterruptedException {
      Result row = getRecordReader().getCurrentValue();
      VertexHandler vertexHandler = getVertexHandler();

      Vertex<LongWritable, BTGVertexValue, NullWritable> vertex =
        getConf().createVertex();

      // vertexID
      LongWritable vertexID =
        new LongWritable(vertexHandler.getVertexID(row.getRow()));

      // vertex type is stored the first label of the vertex
      Integer vertexLabel = Integer.valueOf(vertexHandler.readLabel(row));
      BTGVertexType vertexType = BTGVertexType.values()[vertexLabel];

      // initial vertex value is the vertex id
      Double vertexValue = new Double(String.valueOf(vertexID));

      // btgIDs are the graphs this vertex belongs to
      List<Long> btgIDs = Lists.newArrayList(vertexHandler.readGraphs(row));

      BTGVertexValue btgVertexValue =
        new BTGVertexValue(vertexType, vertexValue, btgIDs);

      // read outgoing edges
      List<Edge<LongWritable, NullWritable>> edges = Lists.newArrayList();
      for (org.gradoop.model.Edge e : vertexHandler.readOutgoingEdges(row)) {
        edges.add(EdgeFactory.create(new LongWritable(e.getOtherID())));
      }
      // read incoming edges
      for (org.gradoop.model.Edge e : vertexHandler.readIncomingEdges(row)) {
        edges.add(EdgeFactory.create(new LongWritable(e.getOtherID())));
      }

      vertex.initialize(vertexID, btgVertexValue, edges);

      return vertex;
    }
  }
}
