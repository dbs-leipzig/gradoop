package org.gradoop.storage.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.gradoop.model.Attributed;
import org.gradoop.model.MultiLabeled;
import org.gradoop.storage.exceptions.UnsupportedTypeException;

import java.util.Map;

/**
 * Created by s1ck on 11/10/14.
 */
public interface EntityHandler {
  Put writeLabels(Put put, MultiLabeled entity);

  Put writeProperties(Put put, Attributed entity)
    throws UnsupportedTypeException;

  Iterable<String> readLabels(Result res);

  Map<String, Object> readProperties(Result res);
}
