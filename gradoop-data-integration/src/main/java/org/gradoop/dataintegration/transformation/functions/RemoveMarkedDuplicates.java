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
package org.gradoop.dataintegration.transformation.functions;

import org.gradoop.common.model.api.entities.Element;
import org.gradoop.flink.model.impl.functions.filters.CombinableFilter;

/**
 * Deletes elements that are duplicates of another element. Duplicates will be identified by having the
 * {@link MarkDuplicatesInGroup#PROPERTY_KEY} property set.<p>
 * This filter function will therefore accept all elements that do not have this property set.
 *
 * @param <E> The element type.
 */
public class RemoveMarkedDuplicates<E extends Element> implements CombinableFilter<E> {

  @Override
  public boolean filter(E value) {
    return !value.hasProperty(MarkDuplicatesInGroup.PROPERTY_KEY);
  }
}
