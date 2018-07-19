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
package org.gradoop.common.storage.impl.hbase.api;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Utility class for common HBase filter tasks
 */
public interface FilterUtils {
  /**
   * Creates a HBase Filter object to return only graph elements that are equal to the given
   * GradoopIds.
   *
   * @param elementIds a set of graph element GradoopIds to filter
   * @return a HBase Filter object
   */
  default Filter getIdFilter(GradoopIdSet elementIds) {
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
}
