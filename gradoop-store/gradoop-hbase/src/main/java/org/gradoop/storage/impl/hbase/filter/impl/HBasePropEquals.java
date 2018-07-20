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
import org.apache.log4j.Logger;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.storage.common.predicate.filter.impl.PropEquals;
import org.gradoop.storage.impl.hbase.filter.api.HBaseElementFilter;
import org.gradoop.storage.impl.hbase.iterator.HBasePropertyValueWrapper;

import javax.annotation.Nonnull;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.gradoop.storage.impl.hbase.constants.HBaseConstants.CF_PROPERTIES;

/**
 * HBase property equality implementation
 *
 * @param <T> EPGM element type
 */
public class HBasePropEquals<T extends EPGMElement> extends PropEquals<HBaseElementFilter<T>>
  implements HBaseElementFilter<T> {

  /**
   * Static logger instance
   */
  private static final Logger LOG = Logger.getLogger(HBasePropEquals.class);

  /**
   * List with class definitions representing property values with dynamic lengths
   */
  private final List dynamicLengthClasses =
    Arrays.asList(String.class, BigDecimal.class, Map.class, List.class);

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
    byte[] rawBytesValue = getValue().getRawBytes();

    // Transform byte representation of values with dynamic length to HBase specific format or skip
    // if transformation is not possible
    if (dynamicLengthClasses.contains(getValue().getType())) {
      HBasePropertyValueWrapper wrapper = new HBasePropertyValueWrapper(getValue());

      try (
        ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream stream = new DataOutputStream(arrayOutputStream)
      ) {
        wrapper.write(stream);
        // overwrite byte array with HBase specific representation of the value
        rawBytesValue = arrayOutputStream.toByteArray();
      } catch (IOException e) {
        LOG.trace(
          "Can not transform property into HBase specific byte format. Skip element.", e);
        return new FilterList();
      }
    }

    SingleColumnValueFilter filter = new SingleColumnValueFilter(
      Bytes.toBytesBinary(CF_PROPERTIES),
      Bytes.toBytesBinary(getKey()),
      CompareFilter.CompareOp.EQUAL,
      rawBytesValue
    );

    // Define that the entire row will be skipped if the column is not found
    filter.setFilterIfMissing(true);

    return filter;
  }
}
