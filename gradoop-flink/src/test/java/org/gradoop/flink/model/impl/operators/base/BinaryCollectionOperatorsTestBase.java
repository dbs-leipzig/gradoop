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
package org.gradoop.flink.model.impl.operators.base;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;

import static org.junit.Assert.assertTrue;

public class BinaryCollectionOperatorsTestBase extends GradoopFlinkTestBase {

  protected void checkAssertions(GraphCollection expectation,
    GraphCollection result, String attribute) throws Exception {
    assertTrue(
      "wrong graph ids for " + attribute + " overlapping collections",
      result.equalsByGraphIds(expectation).collect().get(0));
    assertTrue(
      "wrong graph element ids for" + attribute + " overlapping collections",
      result.equalsByGraphElementIds(expectation).collect().get(0));
  }
}
