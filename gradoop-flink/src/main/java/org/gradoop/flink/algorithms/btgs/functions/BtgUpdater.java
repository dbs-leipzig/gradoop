/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
package org.gradoop.flink.algorithms.btgs.functions;

import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Update Function of vertex centric iteration.
 */
public class BtgUpdater
  extends GatherFunction<GradoopId, GradoopId, GradoopId> {

  @Override
  public void updateVertex(Vertex<GradoopId, GradoopId> vertex,
    MessageIterator<GradoopId> messageIterator) throws Exception {

    GradoopId lastComponent = vertex.getValue();
    GradoopId newComponent = lastComponent;

    for (GradoopId messageComponent : messageIterator) {

      if (messageComponent.compareTo(newComponent) < 0) {
        newComponent = messageComponent;
      }
    }

    if (!lastComponent.equals(newComponent)) {
      setNewVertexValue(newComponent);
    }
  }
}
