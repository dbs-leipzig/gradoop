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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.estimation.Estimator;

/**
 * Represents a single node in a {@link QueryPlan}
 */
public interface PlanNode {

  /**
   * Recursively executed this node and returns the resulting {@link Embedding} data set.
   *
   * @return embeddings
   */
  DataSet<Embedding> execute();

  /**
   * Recursively computes the estimated output cardinality of the data set produced by
   * {@link PlanNode#execute()}.
   *
   * @return estimated cardinality
   */
  Estimator getEstimator();

  /**
   * Returns the updates meta data to the embeddings produced by this node.
   *
   * @return
   */
  EmbeddingMetaData getEmbeddingMetaData();
}
