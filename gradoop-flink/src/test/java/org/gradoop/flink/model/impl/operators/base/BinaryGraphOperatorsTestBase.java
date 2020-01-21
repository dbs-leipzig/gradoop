/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.base;

import org.gradoop.common.model.impl.pojo.EPGMGraphElement;
import org.gradoop.flink.model.GradoopFlinkTestBase;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BinaryGraphOperatorsTestBase extends GradoopFlinkTestBase {

  protected void checkElementMatches(Set<EPGMGraphElement> inElements,
    Set<EPGMGraphElement> outElements) {
    for (EPGMGraphElement outElement : outElements) {
      boolean match = false;

      String elementClassName = outElement.getClass().getSimpleName();

      for (EPGMGraphElement inVertex : inElements) {
        if (outElement.getId().equals(inVertex.getId())) {
          assertEquals(
            "wrong number of graphs for " + elementClassName,
            inVertex.getGraphCount() + 1,
            outElement.getGraphCount()
          );
          match = true;
          break;
        }
      }
      assertTrue("expected " + elementClassName + " not found", match);
    }
  }
}
