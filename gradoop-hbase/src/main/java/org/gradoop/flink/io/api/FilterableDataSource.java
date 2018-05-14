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
package org.gradoop.flink.io.api;

import org.gradoop.flink.io.filter.Expression;
import java.util.List;

/**
 * Data source in with support for filter push-down. A Source
 * extending this interface is able to filter records such
 * that the returned DataSet returns fewer records.
 */
public interface FilterableDataSource {

  /**
   * Returns a copy of the DataSource with added predicates.
   * The predicates parameter is a mutable list of conjunctive
   * predicates that are “offered” to the DataSource.
   *
   * @param predicates a mutable list of conjunctive predicates
   * @return a copy of the DataSource with added predicates
   */
  DataSource applyPredicate(List<Expression> predicates);

  /**
   * Returns true if the applyPredicate() method was called before.
   * Hence, isFilterPushedDown() must return true for all
   * TableSource instances returned from a applyPredicate() call.
   *
   * @return true if the applyPredicate() method was called before
   */
  boolean isFilterPushedDown();

}
