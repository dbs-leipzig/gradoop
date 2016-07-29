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

package org.gradoop.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.model.api.epgm.Element;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Clones an element by replacing its id but keeping label and properties.
 *
 * @param <EL> element type
 */
@FunctionAnnotation.ForwardedFields("label;properties")
public class Clone<EL extends Element> implements MapFunction<EL, EL> {

  @Override
  public EL map(EL el) throws Exception {
    el.setId(GradoopId.get());
    return el;
  }
}
