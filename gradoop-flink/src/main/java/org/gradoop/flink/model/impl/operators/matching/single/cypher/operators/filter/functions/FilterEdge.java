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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.functions;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.GraphElementToEmbedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingRecordMetaData;


import java.util.ArrayList;
import java.util.List;

/**
 * Filters a set of edges by given predicates
 */
public class FilterEdge extends RichFlatMapFunction<Edge, Embedding> {
  /**
   * Predicates used for filtering
   */
  private final CNF predicates;

  /**
   * Stores the properties keys needed to convert the edge into an embedding
   */
  private final List<String> propertyKeys;

  /**
   * Meta data about the edge embedding
   */
  private final EmbeddingRecordMetaData metaData;


  /**
   * New edge filter function
   * @param predicates predicates used for filtering
   */
  public FilterEdge(CNF predicates) {
    this.predicates = predicates;

    String variable = (String) predicates.getVariables().toArray()[0];

    this.propertyKeys = new ArrayList<>();
    propertyKeys.addAll(predicates.getPropertyKeys(variable));

    metaData = new EmbeddingRecordMetaData();
    metaData.updateColumnMapping(variable, 0);

    int i = 0;
    for (String propertyKey : propertyKeys) {
      metaData.updatePropertyMapping(variable, propertyKey, i++);
    }
  }

  @Override
  public void flatMap(Edge edge, Collector<Embedding> out) throws Exception {
    Embedding embedding = GraphElementToEmbedding.convert(edge, propertyKeys);

    if (predicates.evaluate(embedding, metaData)) {
      embedding.clearPropertyData();
      embedding.clearIdListData();
      out.collect(embedding);
    }
  }
}
