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
package org.gradoop.storage.common.predicate.filter.impl;


import org.gradoop.storage.common.predicate.filter.api.ElementFilter;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.StringJoiner;

/**
 * Predicate by label equality
 * return element if and only if:
 *  - label if not null
 *  - given label ranges contain element's label value
 *
 * @param <FilterImpl> filter implement type
 */
public abstract class LabelIn<FilterImpl extends ElementFilter>
  implements ElementFilter<FilterImpl> {

  /**
   * epgm label range
   */
  private final Set<String> labels = new HashSet<>();

  /**
   * Create a new LabelIn predicate
   *
   * @param labels label range
   */
  protected LabelIn(String... labels) {
    assert labels.length > 0;
    Collections.addAll(this.labels, labels);
  }

  @Override
  public String toString() {
    StringJoiner joiner = new StringJoiner(" OR ");
    for (String label : labels) {
      joiner.add(String.format("e.meta.label=`%s`", label));
    }
    return joiner.toString();
  }

  protected Set<String> getLabels() {
    return new HashSet<>(labels);
  }

}
