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
package org.gradoop.flink.model.impl.operators.tostring.functions;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.tostring.tuples.GraphHeadString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * concatenates the sorted string representations of graph heads to represent a
 * collection
 */
public class ConcatGraphHeadStrings
  implements GroupReduceFunction<GraphHeadString, String> {

  @Override
  public void reduce(Iterable<GraphHeadString> graphHeadLabels,
    Collector<String> collector) throws Exception {

    List<String> graphLabels = new ArrayList<>();

    for (GraphHeadString graphHeadString : graphHeadLabels) {
      String graphLabel = graphHeadString.getLabel();
      if (! graphLabel.equals("")) {
        graphLabels.add(graphLabel);
      }
    }

    Collections.sort(graphLabels);

    collector.collect(StringUtils.join(graphLabels, System.lineSeparator()));
  }
}
