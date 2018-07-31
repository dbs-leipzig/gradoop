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
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.storage.common.predicate.filter.impl.PropReg;
import org.gradoop.storage.impl.hbase.filter.api.HBaseElementFilter;

import javax.annotation.Nonnull;
import java.util.regex.Pattern;

import static org.gradoop.storage.impl.hbase.constants.HBaseConstants.CF_PROPERTY_TYPE;
import static org.gradoop.storage.impl.hbase.constants.HBaseConstants.CF_PROPERTY_VALUE;

/**
 * HBase property regex filter implementation
 *
 * @param <T> EPGM element type
 */
public class HBasePropReg<T extends EPGMElement> extends PropReg<HBaseElementFilter<T>>
  implements HBaseElementFilter<T> {

  /**
   * Property regex filter constructor
   *
   * @param key property key
   * @param reg label regex
   */
  public HBasePropReg(@Nonnull String key, @Nonnull Pattern reg) {
    super(key, reg);
  }

  /**
   * {@inheritDoc}
   */
  @Nonnull
  @Override
  public Filter toHBaseFilter() {
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

    SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(
      Bytes.toBytesBinary(CF_PROPERTY_VALUE),
      Bytes.toBytesBinary(getKey()),
      CompareFilter.CompareOp.EQUAL,
      new RegexStringComparator(getReg().pattern()));

    // Define that the entire row will be skipped if the column is not found
    valueFilter.setFilterIfMissing(true);

    SingleColumnValueFilter typeFilter = new SingleColumnValueFilter(
      Bytes.toBytesBinary(CF_PROPERTY_TYPE),
      Bytes.toBytesBinary(getKey()),
      CompareFilter.CompareOp.EQUAL,
      new byte[] {PropertyValue.TYPE_STRING});

    // Define that the entire row will be skipped if the column is not found
    typeFilter.setFilterIfMissing(true);

    filterList.addFilter(typeFilter);
    filterList.addFilter(valueFilter);
    return filterList;
  }
}
