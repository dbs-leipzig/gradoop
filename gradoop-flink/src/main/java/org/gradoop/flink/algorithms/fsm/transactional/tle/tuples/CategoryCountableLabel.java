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
package org.gradoop.flink.algorithms.fsm.transactional.tle.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.api.tuples.Countable;

/**
 * (category, label, frequency)
 */
public class CategoryCountableLabel extends Tuple3<String, String, Long>
  implements Categorizable, Countable {

  /**
   * Default constructor.
   */
  public CategoryCountableLabel() {
  }

  /**
   * Constructor.
   *
   * @param category category
   * @param label label
   * @param count count
   */
  public CategoryCountableLabel(String category, String label, Long count) {
    super(category, label, count);
  }

  @Override
  public String getCategory() {
    return f0;
  }

  @Override
  public void setCategory(String category) {
    f0 = category;
  }

  public String getLabel() {
    return f1;
  }

  public void setLabel(String label) {
    f1 = label;
  }

  @Override
  public long getCount() {
    return f2;
  }

  @Override
  public void setCount(long count) {
    f2 = count;
  }
}
