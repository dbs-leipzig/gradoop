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
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.flink.model.impl.functions.filters.CombinableFilter;

/**
 * Filters a dataset of EPGM elements to those whose id is contained in an id dataset.
 *
 * @param <EL> element type
 */
public class IdInBroadcast<EL extends EPGMElement> extends RichFilterFunction<EL>
  implements CombinableFilter<EL> {

  /**
   * broadcast id set name
   */
  public static final String IDS = "Ids";

  /**
   * graph ids
   */
  protected GradoopIdSet ids;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    ids = GradoopIdSet.fromExisting(getRuntimeContext().getBroadcastVariable(IDS));
  }

  @Override
  public boolean filter(EL identifiable) throws Exception {
    return ids.contains(identifiable.getId());
  }
}
