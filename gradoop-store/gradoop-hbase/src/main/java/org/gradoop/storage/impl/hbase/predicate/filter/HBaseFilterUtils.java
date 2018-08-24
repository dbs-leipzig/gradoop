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
package org.gradoop.storage.impl.hbase.predicate.filter;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.regex.Pattern;

import static org.gradoop.storage.impl.hbase.constants.HBaseConstants.CF_META;
import static org.gradoop.storage.impl.hbase.constants.HBaseConstants.CF_PROPERTY_VALUE;
import static org.gradoop.storage.impl.hbase.constants.HBaseConstants.CF_PROPERTY_TYPE;
import static org.gradoop.storage.impl.hbase.constants.HBaseConstants.COL_LABEL;

/**
 * Utility class for common HBase filter tasks
 */
public class HBaseFilterUtils {
  /**
   * Byte representation of meta data column family
   */
  private static final byte[] CF_META_BYTES = Bytes.toBytesBinary(CF_META);
  /**
   * Byte representation of property value column family
   */
  private static final byte[] CF_PROPERTY_VALUE_BYTES = Bytes.toBytesBinary(CF_PROPERTY_VALUE);
  /**
   * Byte representation of property type column family
   */
  private static final byte[] CF_PROPERTY_TYPE_BYTES = Bytes.toBytesBinary(CF_PROPERTY_TYPE);
  /**
   * Byte representation of column qualifier
   */
  private static final byte[] COL_LABEL_BYTES = Bytes.toBytesBinary(COL_LABEL);

  /**
   * Creates a HBase Filter object to return only graph elements that are equal to the given
   * GradoopIds.
   *
   * @param elementIds a set of graph element GradoopIds to filter
   * @return a HBase Filter object
   */
  public static Filter getIdFilter(GradoopIdSet elementIds) {
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);

    for (GradoopId gradoopId : elementIds) {
      RowFilter rowFilter = new RowFilter(
        CompareFilter.CompareOp.EQUAL,
        new BinaryComparator(gradoopId.toByteArray())
      );
      filterList.addFilter(rowFilter);
    }
    return filterList;
  }

  /**
   * Creates a HBase Filter object representation of labelIn predicate
   *
   * @param labels set of labels to filter for
   * @param negate flag to define if this filter should be negated
   * @return the HBase filter representation
   */
  public static Filter getLabelInFilter(@Nonnull Set<String> labels, boolean negate) {
    // Handle negation
    CompareFilter.CompareOp compareOp = negate ? CompareFilter.CompareOp.NOT_EQUAL :
      CompareFilter.CompareOp.EQUAL;
    FilterList.Operator listOperator = negate ? FilterList.Operator.MUST_PASS_ALL :
      FilterList.Operator.MUST_PASS_ONE;

    FilterList filterList = new FilterList(listOperator);

    labels.stream()
      .map(label -> new SingleColumnValueFilter(CF_META_BYTES, COL_LABEL_BYTES, compareOp,
        Bytes.toBytesBinary(label)))
      .forEach(filterList::addFilter);

    return filterList;
  }

  /**
   * Creates a HBase Filter object representation of labelReg predicate
   *
   * @param reg the pattern to filter for
   * @param negate flag to define if this filter should be negated
   * @return the HBase filter representation
   */
  public static Filter getLabelRegFilter(@Nonnull Pattern reg, boolean negate) {
    return new SingleColumnValueFilter(
      CF_META_BYTES,
      COL_LABEL_BYTES,
      negate ? CompareFilter.CompareOp.NOT_EQUAL : CompareFilter.CompareOp.EQUAL,
      new RegexStringComparator(reg.pattern())
    );
  }

  /**
   * Creates a HBase Filter object representation of propEquals predicate
   *
   * @param key the property key to filter for
   * @param value the value to filter for
   * @param negate flag to define if this filter should be negated
   * @return the HBase filter representation
   */
  public static Filter getPropEqualsFilter(@Nonnull String key, @Nonnull PropertyValue value,
    boolean negate) {
    // Handle negation
    CompareFilter.CompareOp compareOp = negate ? CompareFilter.CompareOp.NOT_EQUAL :
      CompareFilter.CompareOp.EQUAL;
    FilterList.Operator listOperator = negate ? FilterList.Operator.MUST_PASS_ONE :
      FilterList.Operator.MUST_PASS_ALL;

    FilterList filterList = new FilterList(listOperator);

    SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(
      CF_PROPERTY_VALUE_BYTES,
      Bytes.toBytesBinary(key),
      compareOp,
      PropertyValueUtils.Bytes.getRawBytesWithoutType(value));

    // Define that the entire row will be skipped if the column is not found
    valueFilter.setFilterIfMissing(true);

    SingleColumnValueFilter typeFilter = new SingleColumnValueFilter(
      CF_PROPERTY_TYPE_BYTES,
      Bytes.toBytesBinary(key),
      compareOp,
      PropertyValueUtils.Bytes.getTypeByte(value));

    // Define that the entire row will be skipped if the column is not found
    typeFilter.setFilterIfMissing(true);

    filterList.addFilter(valueFilter);
    filterList.addFilter(typeFilter);
    return filterList;
  }

  /**
   * Creates a HBase Filter object representation of propReg predicate
   *
   * @param key the property key to filter for
   * @param reg the pattern to search for
   * @param negate flag to define if this filter should be negated
   * @return the HBase filter representation
   */
  public static Filter getPropRegFilter(@Nonnull String key, @Nonnull Pattern reg, boolean negate) {
    // Handle negation
    CompareFilter.CompareOp compareOp = negate ? CompareFilter.CompareOp.NOT_EQUAL :
      CompareFilter.CompareOp.EQUAL;
    FilterList.Operator listOperator = negate ? FilterList.Operator.MUST_PASS_ONE :
      FilterList.Operator.MUST_PASS_ALL;

    FilterList filterList = new FilterList(listOperator);

    SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(
      CF_PROPERTY_VALUE_BYTES,
      Bytes.toBytesBinary(key),
      compareOp,
      new RegexStringComparator(reg.pattern()));

    // Define that the entire row will be skipped if the column is not found
    valueFilter.setFilterIfMissing(true);

    SingleColumnValueFilter typeFilter = new SingleColumnValueFilter(
      CF_PROPERTY_TYPE_BYTES,
      Bytes.toBytesBinary(key),
      compareOp,
      new byte[] {PropertyValue.TYPE_STRING});

    // Define that the entire row will be skipped if the column is not found
    typeFilter.setFilterIfMissing(true);

    filterList.addFilter(typeFilter);
    filterList.addFilter(valueFilter);
    return filterList;
  }

  /**
   * Creates a HBase Filter object representation of propLargerThan predicate
   *
   * @param key the property key to filter for
   * @param min the property value that defines the minimum of the filter
   * @param include a flag to define if a value that is equal to min should be included
   * @param negate flag to define if this filter should be negated
   * @return the HBase filter representation
   */
  public static Filter getPropLargerThanFilter(@Nonnull String key, @Nonnull PropertyValue min,
    boolean include, boolean negate) {
    // Handle negation
    FilterList.Operator listOperator = negate ? FilterList.Operator.MUST_PASS_ONE :
      FilterList.Operator.MUST_PASS_ALL;

    CompareFilter.CompareOp compareOp;
    if (include) {
      if (negate) {
        compareOp = CompareFilter.CompareOp.LESS;
      } else {
        compareOp = CompareFilter.CompareOp.GREATER_OR_EQUAL;
      }
    } else {
      if (negate) {
        compareOp = CompareFilter.CompareOp.LESS_OR_EQUAL;
      } else {
        compareOp = CompareFilter.CompareOp.GREATER;
      }
    }

    FilterList filterList = new FilterList(listOperator);

    SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(
      CF_PROPERTY_VALUE_BYTES,
      Bytes.toBytesBinary(key),
      compareOp,
      new BinaryComparator(PropertyValueUtils.Bytes.getRawBytesWithoutType(min)));

    // Define that the entire row will be skipped if the column is not found
    valueFilter.setFilterIfMissing(true);

    SingleColumnValueFilter typeFilter = new SingleColumnValueFilter(
      CF_PROPERTY_TYPE_BYTES,
      Bytes.toBytesBinary(key),
      negate ? CompareFilter.CompareOp.NOT_EQUAL : CompareFilter.CompareOp.EQUAL,
      PropertyValueUtils.Bytes.getTypeByte(min));

    // Define that the entire row will be skipped if the column is not found
    typeFilter.setFilterIfMissing(true);

    filterList.addFilter(valueFilter);
    filterList.addFilter(typeFilter);
    return filterList;
  }
}
