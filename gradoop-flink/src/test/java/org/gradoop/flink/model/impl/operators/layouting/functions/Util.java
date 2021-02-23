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

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

import java.util.ArrayList;
import java.util.List;

public class Util {

  static List<GradoopId> generateSubVertices(int count) {
    List<GradoopId> result = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      result.add(GradoopId.get());
    }
    return result;
  }

  static LVertex getDummyVertex(int cellid) {
    LVertex v = new LVertex();
    v.setCellid(cellid);
    return v;
  }

  static LVertex getDummyVertex(int x, int y) throws Exception {
    LVertex v = new LVertex(GradoopId.get(), new Vector(x, y));
    return v;
  }
}
