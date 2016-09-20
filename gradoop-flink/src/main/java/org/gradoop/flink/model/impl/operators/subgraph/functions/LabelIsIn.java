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

package org.gradoop.flink.model.impl.operators.subgraph.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.Element;

import java.util.Collection;

/**
 * Filter function to check if an EPGM element's label is in a white list.
 *
 * @param <EL> element type
 */
public class LabelIsIn<EL extends Element> implements FilterFunction<EL> {

  /**
   * White list of labels.
   */
  private final Collection<String> labels;

  /**
   * Constructor.
   * @param labels white list of labels
   */
  public LabelIsIn(String... labels) {
    this.labels = Sets.newHashSet(labels);
  }

  @Override
  public boolean filter(EL element) throws Exception {
    return labels.contains(element.getLabel());
  }
}
