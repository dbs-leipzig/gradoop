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
package org.gradoop.flink.model.impl.id;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GradoopFlinkTestUtils;
import org.junit.Test;

import static org.gradoop.flink.model.impl.GradoopFlinkTestUtils.writeAndRead;
import static org.junit.Assert.assertEquals;

public class GradoopIdSerializationTest extends GradoopFlinkTestBase {

  @Test
  public void testGradoopIdSerialization() throws Exception {
    GradoopId idIn = GradoopId.get();
    assertEquals("GradoopIds were not equal", idIn,
      GradoopFlinkTestUtils.writeAndRead(idIn));
  }

  @Test
  public void testGradoopIdSetSerialization() throws Exception {
    GradoopIdSet idsIn = GradoopIdSet.fromExisting(
      GradoopId.get(), GradoopId.get());
    assertEquals("GradoopIdSets were not equal", idsIn,
      GradoopFlinkTestUtils.writeAndRead(idsIn));
  }
}
