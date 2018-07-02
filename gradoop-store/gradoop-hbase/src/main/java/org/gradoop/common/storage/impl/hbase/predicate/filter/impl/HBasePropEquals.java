package org.gradoop.common.storage.impl.hbase.predicate.filter.impl;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.storage.impl.hbase.predicate.filter.api.HBaseElementFilter;
import org.gradoop.common.storage.predicate.filter.impl.PropEquals;

import javax.annotation.Nonnull;

import static org.gradoop.common.storage.impl.hbase.constants.HBaseConstants.CF_PROPERTIES;

/**
 * HBase property equality implementation
 *
 * @param <T> EPGM element type
 */
public class HBasePropEquals<T extends EPGMElement> extends PropEquals<HBaseElementFilter<T>>
  implements HBaseElementFilter<T> {

  /**
   * Property equals filter
   *
   * @param key   property key
   * @param value property value
   */
  public HBasePropEquals(@Nonnull String key, @Nonnull Object value) {
    super(key, value);
  }

  @Override
  public Filter toHBaseFilter() {
    return new SingleColumnValueFilter(
      Bytes.toBytesBinary(CF_PROPERTIES),
      Bytes.toBytesBinary(getKey()),
      CompareFilter.CompareOp.EQUAL,
      Bytes.toBytesBinary(getValue().toString())
    );
  }
}
