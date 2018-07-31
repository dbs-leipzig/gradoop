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
package org.gradoop.flink.model.impl.operators.selection.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

/**
 * Applies the given filter function on the graph head of the transaction.
 */
@FunctionAnnotation.ReadFields("f0")
public class FilterTransactions implements FilterFunction<GraphTransaction> {
  /**
   * Filter function
   */
  private final FilterFunction<GraphHead> filterFunction;
  /**
   * Constructor
   *
   * @param filterFunction filter function for graph heads
   */
  public FilterTransactions(FilterFunction<GraphHead> filterFunction) {
    this.filterFunction = filterFunction;
  }

  @Override
  public boolean filter(GraphTransaction graphTransaction) throws Exception {
    return filterFunction.filter(graphTransaction.getGraphHead());
  }
}
