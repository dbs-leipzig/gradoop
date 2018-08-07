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
package org.gradoop.flink.algorithms.fsm.dimspan.comparison;

import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Frequency-based label comparator where lower frequency is smaller.
 */
public class ProportionalLabelComparator implements LabelComparator {

  @Override
  public int compare(WithCount<String> a, WithCount<String> b) {
    int comparison;

    if (a.getCount() < b.getCount()) {
      comparison = -1;
    } else if (a.getCount() > b.getCount()) {
      comparison = 1;
    } else {
      comparison = a.getObject().compareTo(b.getObject());
    }

    return comparison;
  }
}
