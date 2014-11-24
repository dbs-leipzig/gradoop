package org.gradoop.storage.hbase;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.gradoop.model.Edge;
import org.gradoop.model.GraphElement;
import org.gradoop.model.Vertex;

import java.io.IOException;
import java.util.Map;

/**
 * Created by s1ck on 11/8/14.
 */
public interface VertexHandler extends EntityHandler {
  void createVerticesTable(final HBaseAdmin admin,
                           final HTableDescriptor tableDescriptor)
    throws IOException;

  Put writeOutgoingEdges(Put put, Iterable<? extends Edge> edges);

  Put writeIncomingEdges(Put put, Iterable<? extends Edge> edges);

  Put writeGraphs(Put put, GraphElement vertex);

  Iterable<Edge> readOutgoingEdges(Result res);

  Iterable<Edge> readIncomingEdges(Result res);

  Iterable<Long> readGraphs(Result res);
}
