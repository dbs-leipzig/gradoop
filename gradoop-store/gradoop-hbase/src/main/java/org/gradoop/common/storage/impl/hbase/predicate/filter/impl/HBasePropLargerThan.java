/**
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
package org.gradoop.common.storage.impl.hbase.predicate.filter.impl;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.storage.impl.hbase.predicate.filter.api.HBaseElementFilter;
import org.gradoop.common.storage.predicate.filter.impl.PropLargerThan;

import javax.annotation.Nonnull;

import static org.gradoop.common.storage.impl.hbase.constants.HBaseConstants.CF_PROPERTIES;

/**
 * HBase property value compare predicate implement
 *
 * @param <T> EPGM element type
 */
public class HBasePropLargerThan<T extends EPGMElement>
  extends PropLargerThan<HBaseElementFilter<T>> implements HBaseElementFilter<T> {

  /**
   * Create a new property compare filter
   *
   * @param key property key
   * @param min property min value
   * @param include include min value
   */
  public HBasePropLargerThan(String key, Object min, boolean include) {
    super(key, min, include);
  }

  /**
   * {@inheritDoc}
   */
  @Nonnull
  @Override
  public Filter toHBaseFilter() {

    SingleColumnValueFilter filter = new SingleColumnValueFilter(
      Bytes.toBytesBinary(CF_PROPERTIES),
      Bytes.toBytesBinary(getKey()),
      isInclude() ? CompareFilter.CompareOp.GREATER_OR_EQUAL : CompareFilter.CompareOp.GREATER,
      new BinaryComparator(getMin().getRawBytes())
    );

    // Define that the entire row will be skipped if the column is not found
    filter.setFilterIfMissing(true);

    return filter;
  }
}
