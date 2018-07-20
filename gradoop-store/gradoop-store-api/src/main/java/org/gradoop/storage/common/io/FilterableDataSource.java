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
package org.gradoop.storage.common.io;

import org.gradoop.flink.io.api.DataSource;
import org.gradoop.storage.common.predicate.query.ElementQuery;

import javax.annotation.Nonnull;

/**
 * Data source with support for filter push-down. A Source
 * extending this interface is able to filter records such
 * that the returned DataSet returns fewer records.
 *
 * @param <GQuery> graph element filter
 * @param <VQuery> vertex element filter
 * @param <EQuery> edge element filter
 */
public interface FilterableDataSource<
  GQuery extends ElementQuery,
  VQuery extends ElementQuery,
  EQuery extends ElementQuery> extends DataSource {

  /**
   * Returns a copy of the DataSource with added predicates.
   * The predicates parameter is a mutable list of conjunctive
   * predicates that are “offered” to the DataSource.
   *
   * @param query graph head query formula
   * @return a copy of the FilterableDataSource with added predicates
   */

  @Nonnull
  FilterableDataSource<GQuery, VQuery, EQuery> applyGraphPredicate(@Nonnull GQuery query);

  /**
   * Returns a copy of the DataSource with added predicates.
   * The predicates parameter is a mutable list of conjunctive
   * predicates that are “offered” to the DataSource.
   *
   * @param query vertex query formula
   * @return a copy of the FilterableDataSource with added predicates
   */
  @Nonnull
  FilterableDataSource<GQuery, VQuery, EQuery> applyVertexPredicate(@Nonnull VQuery query);

  /**
   * Returns a copy of the DataSource with added predicates.
   * The predicates parameter is a mutable list of conjunctive
   * predicates that are “offered” to the DataSource.
   *
   * @param query edge query formula
   * @return a copy of the FilterableDataSource with added predicates
   */
  @Nonnull
  FilterableDataSource<GQuery, VQuery, EQuery> applyEdgePredicate(@Nonnull EQuery query);

  /**
   * Returns true if the apply*Predicate() method was called before.
   * Hence, isFilterPushedDown() must return true for all
   * TableSource instances returned from a applyPredicate() call.
   *
   * @return true if the apply*Predicate() method was called before
   */
  boolean isFilterPushedDown();

}
