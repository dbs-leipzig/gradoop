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

package org.gradoop.flink.algorithms.fsm.transactional.gspan.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples.GraphEmbeddingsPair;
import org.gradoop.flink.model.impl.tuples.IdWithLabel;
import org.gradoop.flink.representation.common.adjacencylist.AdjacencyListRow;
import org.gradoop.flink.representation.transactional.AdjacencyList;

import java.util.Map;

/**
 * bool => (graph, pattern -> embeddings)
 * workaround for bulk iteration intermediate results
 * graph and map are empty
 */
public class CreateCollector implements MapFunction<Boolean, GraphEmbeddingsPair> {

  @Override
  public GraphEmbeddingsPair map(Boolean aBoolean) throws Exception {

    Map<GradoopId, String> labels = Maps.newHashMapWithExpectedSize(0);
    Map<GradoopId, Properties> properties = Maps.newHashMapWithExpectedSize(0);
    Map<GradoopId, AdjacencyListRow<IdWithLabel, IdWithLabel>> rows =
      Maps.newHashMapWithExpectedSize(0);


    AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel> adjacencyList =
      new AdjacencyList<>(new GraphHead(), labels, properties, rows, rows);

    return new GraphEmbeddingsPair(adjacencyList, Maps.newHashMap());
  }
}
