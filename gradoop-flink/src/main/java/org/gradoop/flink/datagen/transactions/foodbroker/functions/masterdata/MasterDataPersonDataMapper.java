/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.datagen.transactions.foodbroker.functions.masterdata;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.datagen.transactions.foodbroker.config.Constants;
import org.gradoop.flink.datagen.transactions.foodbroker.tuples.RelevantPersonData;

/**
 * Creates a tuple from the given vertex. The tuple consists of the gradoop id and the relevant
 * person data.
 */
public class MasterDataPersonDataMapper
  implements MapFunction<Vertex, Tuple2<GradoopId, RelevantPersonData>> {

  /**
   * Reduce object instantiation.
   */
  private RelevantPersonData reuseRelevantPersonData;

  /**
   * Constructor for object instantiation.
   */
  public MasterDataPersonDataMapper() {
    reuseRelevantPersonData = new RelevantPersonData();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Tuple2<GradoopId, RelevantPersonData> map(Vertex v) throws Exception {
    reuseRelevantPersonData.setQuality(v.getPropertyValue(Constants.QUALITY_KEY).getFloat());
    reuseRelevantPersonData.setCity(v.getPropertyValue(Constants.CITY_KEY).getString());
    reuseRelevantPersonData.setHolding(v.getPropertyValue(Constants.HOLDING_KEY).getString());
    return new Tuple2<>(v.getId(), reuseRelevantPersonData);
  }
}
