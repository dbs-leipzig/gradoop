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
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.junit.Test;

import static org.gradoop.storage.impl.hbase.GradoopHBaseTestBase.PATTERN_VERTEX;
import static org.gradoop.storage.impl.hbase.constants.HBaseConstants.CF_META;
import static org.gradoop.storage.impl.hbase.constants.HBaseConstants.COL_LABEL;
import static org.junit.Assert.assertEquals;

/**
 * Test class for {@link HBaseLabelReg}
 */
public class HBaseLabelRegTest {

  /**
   * Test the toHBaseFilter function
   */
  @Test
  public void testToHBaseFilter() {

    HBaseLabelReg<Vertex> vertexFilter = new HBaseLabelReg<>(PATTERN_VERTEX);

    Filter expectedFilter = new SingleColumnValueFilter(
      Bytes.toBytesBinary(CF_META),
      Bytes.toBytesBinary(COL_LABEL),
      CompareFilter.CompareOp.EQUAL,
      new RegexStringComparator(PATTERN_VERTEX.pattern())
    );

    assertEquals(expectedFilter.toString(), vertexFilter.toHBaseFilter(false).toString());
  }
}
