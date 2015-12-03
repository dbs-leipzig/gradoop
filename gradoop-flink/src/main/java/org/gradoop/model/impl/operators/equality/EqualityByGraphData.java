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

package org.gradoop.model.impl.operators.equality;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.operators.equality.functions.DataLabelWithCount;
import org.gradoop.model.impl.operators.equality.functions.GraphHeadDataLabeler;
import org.gradoop.model.impl.operators.equality.functions.LabelAppender;
import org.gradoop.model.impl.operators.equality.tuples.DataLabel;

/**
 * Two collections are equal,
 * if there exists an 1:1 mapping between graphs, where graph label and
 * properties are equal and for each of those pairs,
 * there exists an isomorphism based on element label and property equality.
 *
 * @param <G> graph head type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class EqualityByGraphData
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends EqualityByGraphElementData<G, V, E> {

  @Override
  protected DataSet<Tuple2<String, Long>> labelGraphs(
    GraphCollection<G, V, E> collection) {

    DataSet<DataLabel> graphDataLabels = getElementDataLabels(collection);
    DataSet<DataLabel> graphHeadLabels = collection.getGraphHeads()
      .map(new GraphHeadDataLabeler<G>());

    return graphHeadLabels
      .join(graphDataLabels)
      .where(1).equalTo(0)
      .with(new LabelAppender())
      .map(new DataLabelWithCount())
      .groupBy(0)
      .sum(1);
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
