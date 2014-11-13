package org.gradoop.core.storage.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.gradoop.core.model.Attributed;
import org.gradoop.core.model.Labeled;
import org.gradoop.core.storage.exceptions.UnsupportedTypeException;

import java.util.Map;

/**
 * Created by s1ck on 11/10/14.
 */
public interface EntityHandler {
  Put writeLabels(Put put, Labeled entity);

  Put writeProperties(Put put, Attributed entity) throws UnsupportedTypeException;

  Iterable<String> readLabels(Result res);

  Map<String, Object> readProperties(Result res);
}
