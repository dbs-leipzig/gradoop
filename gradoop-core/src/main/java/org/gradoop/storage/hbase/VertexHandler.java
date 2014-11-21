package org.gradoop.storage.hbase;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
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

  Put writeOutgoingEdges(Put put, Vertex vertex);

  Put writeIncomingEdges(Put put, Vertex vertex);

  Put writeGraphs(Put put, GraphElement vertex);

  Map<String, Map<String, Object>> readOutgoingEdges(Result res);

  Map<String, Map<String, Object>> readIncomingEdges(Result res);

  Iterable<Long> readGraphs(Result res);
}
