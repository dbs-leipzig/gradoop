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
package org.gradoop.flink.algorithms.gelly.randomjump.functions.datasetiteration;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Changes the flag for the active tuple in the iteration dataset. Sets the tuple with the
 * source vertex id provided in {@link #nextActiveVertexBroadcastSet} as active, sets all other
 * tuple as inactive.
 */
public class SetNextVertexActiveRichMap extends
  RichMapFunction<IterativeTuple, IterativeTuple> {

  /**
   * Name of the broadcast set containing the next active vertex id
   */
  private final String nextActiveVertexBroadcastSet;

  /**
   * Set containing the next active vertex id
   */
  private GradoopIdSet nextActiveVertexId;

  /**
   * Creates an instance of SetNextVertexActiveRichMap
   *
   * @param nextActiveVertexBroadcastSet Name of the broadcast set containing the next
   *                                     active vertex id
   */
  public SetNextVertexActiveRichMap(String nextActiveVertexBroadcastSet) {
    this.nextActiveVertexBroadcastSet = nextActiveVertexBroadcastSet;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    nextActiveVertexId = GradoopIdSet.fromExisting(getRuntimeContext().getBroadcastVariable(
      nextActiveVertexBroadcastSet));
  }

  @Override
  public IterativeTuple map(IterativeTuple iterativeTuple) throws Exception {
    if (nextActiveVertexId.contains(iterativeTuple.getSourceId())) {
      iterativeTuple.setTupleActive();
    } else {
      iterativeTuple.setTupleInactive();
    }
    return iterativeTuple;
  }
}
