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
package org.gradoop.flink.model.impl.operators.subgraph.functions;

import com.google.common.collect.Sets;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.flink.model.impl.functions.filters.CombinableFilter;

import java.util.Collection;

/**
 * Filter function to check if an EPGM element's label is in a white list.
 *
 * @param <EL> element type
 */
public class LabelIsIn<EL extends Element> implements CombinableFilter<EL> {

  /**
   * White list of labels.
   */
  private final Collection<String> labels;

  /**
   * Constructor.
   * @param labels white list of labels
   */
  public LabelIsIn(String... labels) {
    this.labels = Sets.newHashSet(labels);
  }

  @Override
  public boolean filter(EL element) throws Exception {
    return labels.contains(element.getLabel());
  }
}
