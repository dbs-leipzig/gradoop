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
package org.gradoop.storage.utils;

import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.storage.impl.hbase.predicate.filter.impl.HBaseLabelIn;
import org.gradoop.storage.impl.hbase.predicate.filter.impl.HBaseLabelReg;
import org.gradoop.storage.impl.hbase.predicate.filter.impl.HBasePropEquals;
import org.gradoop.storage.impl.hbase.predicate.filter.impl.HBasePropLargerThan;
import org.gradoop.storage.impl.hbase.predicate.filter.impl.HBasePropReg;

import javax.annotation.Nonnull;
import java.util.regex.Pattern;

/**
 * HBase filter collection utilities
 */
public class HBaseFilters {

  /**
   * Static generator function for labelIn predicate to apply on a HBase store.
   * See {@link HBaseLabelIn} for further details.
   *
   * @param value value ranges
   * @param <T> epgm element type
   * @return HBaseLabelIn filter instance
   */
  @Nonnull
  public static <T extends EPGMElement> HBaseLabelIn<T> labelIn(@Nonnull String... value) {
    return new HBaseLabelIn<>(value);
  }

  /**
   * Static generator function for labelReg predicate to apply on a HBase store.
   * See {@link HBaseLabelReg} for further details.
   *
   * @param reg regex pattern
   * @param <T> epgm element type
   * @return HBaseLabelReg filter instance
   */
  @Nonnull
  public static <T extends EPGMElement> HBaseLabelReg<T> labelReg(@Nonnull Pattern reg) {
    return new HBaseLabelReg<>(reg);
  }

  /**
   * Static generator function for propEquals predicate to apply on a HBase store.
   * See {@link HBasePropEquals} for further details.
   *
   * @param key property key
   * @param value property value
   * @param <T> epgm element type
   * @return HBasePropEquals filter instance
   */
  @Nonnull
  public static <T extends EPGMElement> HBasePropEquals<T> propEquals(
    @Nonnull String key,
    @Nonnull Object value
  ) {
    return new HBasePropEquals<>(key, value);
  }

  /**
   * Static generator function for propLargerThan predicate to apply on a HBase store.
   * See {@link HBasePropLargerThan} for further details.
   *
   * @param key property key
   * @param value property value
   * @param include should include value
   * @param <T> epgm element type
   * @return HBasePropLargerThan filter instance
   */
  @Nonnull
  public static <T extends EPGMElement> HBasePropLargerThan<T> propLargerThan(
    @Nonnull String key,
    Object value,
    boolean include) {
    return new HBasePropLargerThan<>(key, value, include);
  }

  /**
   * Static generator function for propReg predicate to apply on a HBase store.
   * See {@link HBasePropReg} for further details.
   *
   * @param key property key
   * @param pattern property pattern
   * @param <T> epgm element type
   * @return HBasePropReg filter instance
   */
  @Nonnull
  public static <T extends EPGMElement> HBasePropReg<T> propReg(
    @Nonnull String key,
    @Nonnull Pattern pattern) {
    return new HBasePropReg<>(key, pattern);
  }
}
