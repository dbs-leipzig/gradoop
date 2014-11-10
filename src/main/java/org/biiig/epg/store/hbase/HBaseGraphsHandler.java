package org.biiig.epg.store.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.biiig.epg.model.Graph;

/**
 * Created by s1ck on 11/10/14.
 */
public interface HBaseGraphsHandler extends HBaseEntityHandler {
  Put writeVertices(Put put, Graph graph);

  Iterable<Long> readVertices(Result res);
}