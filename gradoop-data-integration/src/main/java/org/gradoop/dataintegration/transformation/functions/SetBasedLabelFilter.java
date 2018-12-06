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
package org.gradoop.dataintegration.transformation.functions;

import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.flink.model.impl.functions.filters.CombinableFilter;

import java.util.Set;

/**
 * A simple Filter which only excepts entries that are present in a given {@link Set}.
 *
 * @param <T> The {@link EPGMElement} the filter is used for.
 */
public class SetBasedLabelFilter<T extends EPGMElement> implements CombinableFilter<T> {

  /**
   * The Set of labels that are accepted.
   */
  private final Set<String> labels;

  /**
   * The constructor of the {@link SetBasedLabelFilter}.
   *
   * @param labels The labels that are accepted by the filter.
   */
  public SetBasedLabelFilter(Set<String> labels) {
    this.labels = labels;
  }

  @Override
  public boolean filter(T e) {
    return labels.contains(e.getLabel());
  }
}