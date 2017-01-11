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
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.GraphElementToEmbedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingRecord;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingRecordMetaData;


import java.util.ArrayList;
import java.util.List;

/**
 * Filters a set of vertices by given predicates
 */
public class FilterVertex extends RichFlatMapFunction<Vertex, EmbeddingRecord> {
  /**
   * Predicates used for filtering
   */
  private final CNF predicates;

  private final List<String> propertyKeys;

  private final EmbeddingRecordMetaData metaData;

  /**
   * New vertex filter function
   * @param predicates predicates used for filtering
   */
  public FilterVertex(CNF predicates) {
    this.predicates = predicates;

    String variable = (String) predicates.getVariables().toArray()[0];
    this.propertyKeys = new ArrayList<>();
    propertyKeys.addAll(predicates.getProperties(variable));

    metaData = new EmbeddingRecordMetaData();
    metaData.updateColumnMapping(variable,0);

    int i = 0;
    for(String propertyKey : propertyKeys) {
      metaData.updatePropertyMapping(variable, propertyKey, i++);
    }
  }

  @Override
  public void flatMap(Vertex vertex, Collector<EmbeddingRecord> out) throws Exception {
    EmbeddingRecord embedding = GraphElementToEmbedding.convert(vertex, propertyKeys);

    if (predicates.evaluate(embedding, metaData)) {
      embedding.clearPropertyData();
      embedding.clearIdListData();
      out.collect(embedding);
    }
  }
}
