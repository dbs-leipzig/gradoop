/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.storage.hbase.impl.predicate.filter.api;

import org.apache.hadoop.hbase.filter.Filter;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.storage.common.predicate.filter.api.ElementFilter;
import org.gradoop.storage.hbase.impl.predicate.filter.calculate.And;
import org.gradoop.storage.hbase.impl.predicate.filter.calculate.Not;
import org.gradoop.storage.hbase.impl.predicate.filter.calculate.Or;

import javax.annotation.Nonnull;
import java.io.Serializable;

/**
 * HBase Element Filter interface to chain predicates
 *
 * @param <T> element type
 */
public interface HBaseElementFilter<T extends Element>
  extends ElementFilter<HBaseElementFilter<T>>, Serializable {

  @Nonnull
  @Override
  default HBaseElementFilter<T> or(@Nonnull HBaseElementFilter<T> another) {
    return Or.create(this, another);
  }

  @Nonnull
  @Override
  default HBaseElementFilter<T> and(@Nonnull HBaseElementFilter<T> another) {
    return And.create(this, another);
  }

  @Nonnull
  @Override
  default HBaseElementFilter<T> negate() {
    return Not.of(this);
  }

  /**
   * Translate the filter to a HBase specific {@link Filter} which can be applied
   * to a {@link org.apache.hadoop.hbase.client.Scan} instance.
   *
   * @param negate flag to negate the filter
   * @return the translated filter instance
   */
  @Nonnull
  Filter toHBaseFilter(boolean negate);
}
