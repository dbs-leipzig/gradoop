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
package org.gradoop.storage.impl.hbase.predicate.filter.impl;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.impl.pojo.Edge;
import org.junit.Test;

import java.util.Arrays;

import static org.gradoop.storage.impl.hbase.constants.HBaseConstants.CF_META;
import static org.gradoop.storage.impl.hbase.constants.HBaseConstants.COL_LABEL;
import static org.junit.Assert.assertEquals;

/**
 * Test class for {@link HBaseLabelIn}
 */
public class HBaseLabelInTest {

  /**
   * Test the toHBaseFilter function
   */
  @Test
  public void testToHBaseFilter() {
    String testLabel1 = "test1";
    String testLabel2 = "test2";

    HBaseLabelIn<Edge> edgeFilter = new HBaseLabelIn<>(testLabel1, testLabel2);

    FilterList expectedFilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);

    for (String label : Arrays.asList(testLabel2, testLabel1)) {
      SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(
        Bytes.toBytesBinary(CF_META),
        Bytes.toBytesBinary(COL_LABEL),
        CompareFilter.CompareOp.EQUAL,
        Bytes.toBytesBinary(label)
      );
      expectedFilterList.addFilter(valueFilter);
    }
    assertEquals(expectedFilterList.toString(), edgeFilter.toHBaseFilter(false).toString());
  }
}
