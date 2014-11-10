package org.biiig.epg.store.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.biiig.epg.model.Attributed;
import org.biiig.epg.model.Labeled;

import java.util.Map;

/**
 * Created by s1ck on 11/10/14.
 */
public interface HBaseEntityHandler {
  Put writeLabels(Put put, Labeled entity);

  Put writeProperties(Put put, Attributed entity);

  Iterable<String> readLabels(Result res);

  Map<String, Object> readProperties(Result res);
}
