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
package org.gradoop.flink.model.impl.functions.graphcontainment;

import org.gradoop.common.model.api.entities.EPGMGraphElement;
import org.gradoop.flink.model.impl.functions.filters.CombinableFilter;

/**
 * True, if the element has not graph ids.
 *
 * @param <EL> epgm graph element
 */
public class InNoGraph<EL extends EPGMGraphElement> implements CombinableFilter<EL> {

  @Override
  public boolean filter(EL value) throws Exception {
    return value.getGraphIds() == null || value.getGraphIds().isEmpty();
  }
}
