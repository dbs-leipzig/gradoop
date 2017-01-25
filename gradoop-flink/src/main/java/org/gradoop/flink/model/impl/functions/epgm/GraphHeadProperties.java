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
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMLabeled;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * EPGMGraphHead with properties => properties
 *
 * @param <L> EPGMGraphHead type having properties
 */
@FunctionAnnotation.ForwardedFields("label->*")
public class GraphHeadProperties<L extends EPGMGraphHead>
  implements MapFunction<L, Properties>, KeySelector<L, Properties> {

  @Override
  public Properties map(L l) throws Exception {
    return l.getProperties();
  }

  @Override
  public Properties getKey(L l) throws Exception {
    return l.getProperties();
  }
}
