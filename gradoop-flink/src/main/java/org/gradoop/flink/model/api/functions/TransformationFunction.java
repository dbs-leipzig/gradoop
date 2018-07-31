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
package org.gradoop.flink.model.api.functions;

import org.gradoop.common.model.api.entities.EPGMAttributed;
import org.gradoop.common.model.api.entities.EPGMLabeled;

import java.io.Serializable;

/**
 * A serializable function that is applied on an EPGM element (i.e. graph head,
 * vertex and edge) to transform its data, but not its identity.
 *
 * @param <EL> EPGM attributed / labeled element
 */
public interface TransformationFunction<EL extends EPGMAttributed & EPGMLabeled>
  extends Serializable {

  /**
   * The method takes the current version of the element and a copy of that
   * element as input. The copy is initialized with the current structural
   * information (i.e. identifiers, graph membership, source / target
   * identifiers). The implementation is able to transform the element by either
   * updating the current version and return it or by adding necessary
   * information to the new entity and return it.
   *
   * @param current       current element
   * @param transformed   structural identical, but plain element
   * @return transformed element
   */
  EL apply(EL current, EL transformed);

  /**
   * Returns the unmodified element.
   *
   * @param <EL> EPGM attributed / labeled element
   * @return a function that always returns the current element
   */
  static <EL extends EPGMAttributed & EPGMLabeled> TransformationFunction<EL>
  keep() {
    return (c, t) -> c;
  }
}
