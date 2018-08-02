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
package org.gradoop.storage.impl.hbase.io.inputformats;

import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.storage.common.predicate.query.ElementQuery;
import org.gradoop.storage.impl.hbase.predicate.filter.HBaseFilterUtils;
import org.gradoop.storage.impl.hbase.predicate.filter.api.HBaseElementFilter;

import javax.annotation.Nonnull;

/**
 * Base class for common functionality of HBase input formats
 *
 * @param <E> type of EPGM element
 */
abstract class BaseTableInputFormat<E extends EPGMElement> extends TableInputFormat<Tuple1<E>> {

  /**
   * Attach a HBase filter represented by the given query to the given scan instance.
   *
   * @param query the query that represents a filter
   * @param scan the HBase scan instance on which the filter will be applied
   */
  void attachFilter(@Nonnull ElementQuery<HBaseElementFilter<E>> query, @Nonnull Scan scan) {
    FilterList conjunctFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);

    if (query.getQueryRanges() != null && !query.getQueryRanges().isEmpty()) {
      conjunctFilters.addFilter(HBaseFilterUtils.getIdFilter(query.getQueryRanges()));
    }

    if (query.getFilterPredicate() != null) {
      conjunctFilters.addFilter(query.getFilterPredicate().toHBaseFilter(false));
    }

    // if there are filters inside the root list, add it to the Scan object
    if (!conjunctFilters.getFilters().isEmpty()) {
      scan.setFilter(conjunctFilters);
    }
  }
}
