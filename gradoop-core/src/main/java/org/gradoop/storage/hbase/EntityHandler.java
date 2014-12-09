package org.gradoop.storage.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.gradoop.model.Attributed;
import org.gradoop.model.MultiLabeled;
import org.gradoop.storage.exceptions.UnsupportedTypeException;

import java.util.Map;

/**
 * Handles writing and reading labels and properties of an epg entity, normally
 * vertices and graphs.
 */
public interface EntityHandler {
  /**
   * Adds all labels to the given HBase put and returns it.
   *
   * @param put    put to write labels to
   * @param entity entity to use the labels from
   * @return put with labels
   */
  Put writeLabels(final Put put, final MultiLabeled entity);

  /**
   * Adds all properties to the given HBase put and returns it.
   *
   * @param put    put to write properties to
   * @param entity entity to use properties from
   * @return put with properties
   * @throws UnsupportedTypeException
   */
  Put writeProperties(final Put put, final Attributed entity);

  /**
   * Reads all labels from the given HBase row result.
   *
   * @param res HBase row result
   * @return all labels contained in the row
   */
  Iterable<String> readLabels(final Result res);

  /**
   * Reads all properties from the given HBase row result.
   *
   * @param res HBase row result
   * @return all properties contained in the row
   */
  Map<String, Object> readProperties(final Result res);
}
