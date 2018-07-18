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
package org.gradoop.common.storage.impl.hbase.predicate.filter.api;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.filter.Filter;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.storage.predicate.filter.api.ElementFilter;

import javax.annotation.Nonnull;
import java.io.Serializable;

/**
 * HBase Element Filter interface to chain predicates
 *
 * @param <T> EPGM element type
 */
public interface HBaseElementFilter<T extends EPGMElement>
  extends ElementFilter<HBaseElementFilter<T>>, Serializable {

  @Nonnull
  @Override
  default HBaseElementFilter<T> or(@Nonnull HBaseElementFilter<T> another) {
    // this will be implemented at issue #857
    throw new NotImplementedException("Logical 'or' not implemented.");
  }

  @Nonnull
  @Override
  default HBaseElementFilter<T> and(@Nonnull HBaseElementFilter<T> another) {
    // this will be implemented at issue #857
    throw new NotImplementedException("Logical 'and' not implemented.");
  }

  @Nonnull
  @Override
  default HBaseElementFilter<T> negate() {
    // this will be implemented at issue #857
    throw new NotImplementedException("Logical negation not implemented.");
  }

  /**
   * Translate the filter to a HBase specific {@link Filter} which can be applied
   * to a {@link org.apache.hadoop.hbase.client.Scan} instance.
   *
   * @return the translated filter instance
   */
  @Nonnull
  Filter toHBaseFilter();
}
