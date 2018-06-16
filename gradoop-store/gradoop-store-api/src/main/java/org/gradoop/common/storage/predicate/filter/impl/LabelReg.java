/**
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
package org.gradoop.common.storage.predicate.filter.impl;

import org.gradoop.common.storage.predicate.filter.api.ElementFilter;

import java.util.regex.Pattern;

/**
 * label filter by regex
 *
 * @param <FilterImpl> filter implement type
 */
public abstract class LabelReg<FilterImpl extends ElementFilter>
  implements ElementFilter<FilterImpl> {

  /**
   * regex pattern
   */
  private final Pattern reg;

  /**
   * label regex filter constructor
   *
   * @param reg label regex
   */
  public LabelReg(Pattern reg) {
    this.reg = reg;
  }

  @Override
  public String toString() {
    return String.format("e.meta.label SIMILAR TO `%s`", reg.pattern());
  }

  protected Pattern getReg() {
    return reg;
  }

}
