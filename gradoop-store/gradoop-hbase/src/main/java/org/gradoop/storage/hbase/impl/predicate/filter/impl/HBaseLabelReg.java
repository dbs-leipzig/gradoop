/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.storage.hbase.impl.predicate.filter.impl;

import org.apache.hadoop.hbase.filter.Filter;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.storage.common.predicate.filter.impl.LabelReg;
import org.gradoop.storage.hbase.impl.predicate.filter.HBaseFilterUtils;
import org.gradoop.storage.hbase.impl.predicate.filter.api.HBaseElementFilter;

import javax.annotation.Nonnull;
import java.util.regex.Pattern;

/**
 * HBase label regex predicate implementation
 *
 * @param <T> element type
 */
public class HBaseLabelReg<T extends Element> extends LabelReg<HBaseElementFilter<T>>
  implements HBaseElementFilter<T> {

  /**
   * Create a new LabelReg predicate
   *
   * @param reg label regex pattern
   */
  public HBaseLabelReg(Pattern reg) {
    super(reg);
  }

  @Nonnull
  @Override
  public Filter toHBaseFilter(boolean negate) {
    return HBaseFilterUtils.getLabelRegFilter(getReg(), negate);
  }
}
