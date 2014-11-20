package org.gradoop.storage.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.gradoop.model.Graph;

/**
 * Created by s1ck on 11/10/14.
 */
public interface GraphHandler extends EntityHandler {
  Put writeVertices(Put put, Graph graph);

  Iterable<Long> readVertices(Result res);
}