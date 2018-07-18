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
package org.gradoop.flink.model.impl.functions.epgm;

import org.gradoop.common.model.api.entities.EPGMLabeled;
import org.gradoop.flink.model.impl.functions.filters.CombinableFilter;

/**
 * Accepts all elements which have the same label as specified.
 *
 * @param <L> EPGM labeled type
 */
public class ByLabel<L extends EPGMLabeled> implements CombinableFilter<L> {
  /**
   * Label to be filtered on.
   */
  private String label;

  /**
   * Valued constructor.
   *
   * @param label label to be filtered on
   */
  public ByLabel(String label) {
    this.label = label;
  }

  @Override
  public boolean filter(L l) throws Exception {
    return l.getLabel().equals(label);
  }
}
