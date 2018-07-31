/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.storage.impl.hbase.filter.impl;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;
import org.gradoop.storage.common.predicate.filter.impl.PropEquals;
import org.gradoop.storage.impl.hbase.filter.api.HBaseElementFilter;

import javax.annotation.Nonnull;

import static org.gradoop.storage.impl.hbase.constants.HBaseConstants.CF_PROPERTY_TYPE;
import static org.gradoop.storage.impl.hbase.constants.HBaseConstants.CF_PROPERTY_VALUE;

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

  /**
   * {@inheritDoc}
   */
  @Override
  @Nonnull
  public Filter toHBaseFilter() {
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

    SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(
      Bytes.toBytesBinary(CF_PROPERTY_VALUE),
      Bytes.toBytesBinary(getKey()),
      CompareFilter.CompareOp.EQUAL,
      PropertyValueUtils.Bytes.getRawBytesWithoutType(getValue()));

    // Define that the entire row will be skipped if the column is not found
    valueFilter.setFilterIfMissing(true);

    SingleColumnValueFilter typeFilter = new SingleColumnValueFilter(
      Bytes.toBytesBinary(CF_PROPERTY_TYPE),
      Bytes.toBytesBinary(getKey()),
      CompareFilter.CompareOp.EQUAL,
      PropertyValueUtils.Bytes.getTypeByte(getValue()));

    // Define that the entire row will be skipped if the column is not found
    typeFilter.setFilterIfMissing(true);

    filterList.addFilter(valueFilter);
    filterList.addFilter(typeFilter);
    return filterList;
  }
}
