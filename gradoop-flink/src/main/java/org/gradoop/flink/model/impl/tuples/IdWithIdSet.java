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
package org.gradoop.flink.model.impl.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * (id, {id, id, ...})
 */
public class IdWithIdSet extends Tuple2<GradoopId, GradoopIdSet> {

  public GradoopId getId() {
    return f0;
  }

  public void setId(GradoopId id) {
    f0 = id;
  }

  public GradoopIdSet getIdSet() {
    return f1;
  }

  public void setIdSet(GradoopIdSet idSet) {
    f1 = idSet;
  }
}
