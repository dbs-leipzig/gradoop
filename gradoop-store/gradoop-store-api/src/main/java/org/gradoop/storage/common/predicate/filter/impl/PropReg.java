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

import javax.annotation.Nonnull;
import java.util.regex.Pattern;

/**
 * Predicate by property value regex
 * return element if and only if:
 *  - contains property value
 *  - property value type is string
 *  - property value match given regex formula
 *
 * @param <FilterImpl> filter implement type
 */
public abstract class PropReg<FilterImpl extends ElementFilter>
  implements ElementFilter<FilterImpl> {

  /**
   * regex pattern
   */
  private final Pattern reg;

  /**
   * property key name
   */
  private final String key;

  /**
   * Property regex filter constructor
   *
   * @param key property key
   * @param reg label regex
   */
  public PropReg(
    @Nonnull String key,
    @Nonnull Pattern reg
  ) {
    this.key = key;
    this.reg = reg;
  }

  @Override
  public String toString() {
    return String.format("e.prop.%1$s REGEXP `%2$s`", key, reg.pattern());
  }

  protected Pattern getReg() {
    return reg;
  }

  protected String getKey() {
    return key;
  }

}
