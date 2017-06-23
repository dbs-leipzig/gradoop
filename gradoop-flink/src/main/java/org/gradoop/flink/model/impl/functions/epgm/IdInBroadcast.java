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
package org.gradoop.flink.model.impl.functions.epgm;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Collection;

/**
 * Filters a dataset of EPGM elements to those whose id is contained in an id dataset.
 *
 * @param <EL> element type
 */
public class IdInBroadcast<EL extends EPGMElement> extends RichFilterFunction<EL> {

  /**
   * broadcast id set name
   */
  public static final String IDS = "Ids";

  /**
   * graph ids
   */
  protected Collection<GradoopId> ids;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    ids = getRuntimeContext().getBroadcastVariable(IDS);
    ids = Sets.newHashSet(ids);
  }

  @Override
  public boolean filter(EL identifiable) throws Exception {
    return ids.contains(identifiable.getId());
  }
}
