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
package org.gradoop.storage.common.predicate.filter.api;

import javax.annotation.Nonnull;
import java.io.Serializable;

/**
 * Element Filter Formula
 * A element filter predicate will be
 *  - created by client,
 *  - transform to query options and send to region servers
 *  - decode query options by region servers,
 *
 * @param <FilterImpl> filter implement type
 */
public interface ElementFilter<FilterImpl extends ElementFilter> extends Serializable {

  /**
   * Conjunctive operator for element filter
   *
   * @param another another reduce filter
   * @return conjunctive logic filter
   */
  @Nonnull
  FilterImpl or(@Nonnull FilterImpl another);

  /**
   * Disjunctive operator for element filter
   *
   * @param another another reduce filter
   * @return disjunctive logic filter
   */
  @Nonnull
  FilterImpl and(@Nonnull FilterImpl another);

  /**
   * Negative operator for element filter
   *
   * @return negative logic for current filter
   */
  @Nonnull
  FilterImpl negate();

}
