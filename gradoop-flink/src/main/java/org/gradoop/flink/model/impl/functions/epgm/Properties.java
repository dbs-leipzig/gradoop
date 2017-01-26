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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.api.entities.EPGMElement;
/**
 * EPGMElement with properties => properties
 *
 * @param <L> EPGMElement type having properties
 */
@FunctionAnnotation.ForwardedFields("properties->*")
public class Properties<L extends EPGMElement>
  implements MapFunction<L, org.gradoop.common.model.impl.properties.Properties>,
             KeySelector<L, org.gradoop.common.model.impl.properties.Properties> {

  @Override
  public org.gradoop.common.model.impl.properties.Properties map(L l) throws Exception {
    return l.getProperties();
  }

  @Override
  public org.gradoop.common.model.impl.properties.Properties getKey(L l) throws Exception {
    return l.getProperties();
  }
}
