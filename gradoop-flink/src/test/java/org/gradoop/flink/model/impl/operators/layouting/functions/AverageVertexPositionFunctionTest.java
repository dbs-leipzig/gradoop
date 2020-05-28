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
package org.gradoop.flink.model.impl.operators.layouting.functions;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class AverageVertexPositionFunctionTest extends GradoopFlinkTestBase {
  @Test
  public void testAverageCalculation() throws Exception {
    DataSet<LVertex> tv = getExecutionEnvironment()
      .fromElements(new LVertex(GradoopId.get(), new Vector(10, 10)),
        new LVertex(GradoopId.get(), new Vector(20, 20)),
        new LVertex(GradoopId.get(), new Vector(30, 30)));
    List<Vector> avg = new AverageVertexPositionsFunction().averagePosition(tv).collect();
    Assert.assertEquals(1, avg.size());
    Assert.assertEquals(new Vector(20, 20), avg.get(0));
  }
}
