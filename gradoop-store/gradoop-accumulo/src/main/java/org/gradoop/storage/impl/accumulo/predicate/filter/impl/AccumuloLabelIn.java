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
package org.gradoop.storage.impl.accumulo.predicate.filter.impl;

import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.storage.common.predicate.filter.impl.LabelIn;
import org.gradoop.storage.impl.accumulo.predicate.filter.api.AccumuloElementFilter;

/**
 * Accumulo label equality predicate implement
 *
 * @param <T> EPGM element type
 */
public class AccumuloLabelIn<T extends EPGMElement>
  extends LabelIn<AccumuloElementFilter<T>>
  implements AccumuloElementFilter<T> {

  /**
   * Create a new label equality filter
   *
   * @param labels label
   */
  public AccumuloLabelIn(String... labels) {
    super(labels);
  }

  @Override
  public boolean test(T t) {
    return t.getLabel() != null && getLabels().contains(t.getLabel());
  }

}
