/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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

import com.google.common.collect.Sets;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.flink.model.impl.functions.filters.CombinableFilter;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;

/**
 * Filter function to check if an EPGM elements label is in a white list.
 *
 * @param <EL> The element type to filter.
 */
public class LabelIsIn<EL extends EPGMElement> implements CombinableFilter<EL> {

  /**
   * White list of labels.
   */
  private final Collection<String> labels;

  /**
   * Constructor for this filter using an array of labels.
   *
   * @param labels white list of labels
   */
  public LabelIsIn(String... labels) {
    this.labels = Sets.newHashSet(labels);
  }

  /**
   * Constructor for this filter using a collection of labels.
   *
   * @param labels A collection of accepted labels.
   */
  public LabelIsIn(Collection<String> labels) {
    Objects.requireNonNull(labels);
    this.labels = new HashSet<>(labels);
  }

  @Override
  public boolean filter(EL element) throws Exception {
    return labels.contains(element.getLabel());
  }
}
