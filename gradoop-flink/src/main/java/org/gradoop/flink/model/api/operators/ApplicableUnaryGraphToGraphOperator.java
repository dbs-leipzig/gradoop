/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.api.operators;

import org.gradoop.flink.model.api.epgm.GraphCollection;

/**
 * A marker interface for instances of {@link UnaryGraphToGraphOperator} that
 * support the application on each element in a graph collection.
 */
public interface ApplicableUnaryGraphToGraphOperator
  extends UnaryCollectionToCollectionOperator {

  @Override
  default GraphCollection execute(GraphCollection collection) {
    return (collection.isTransactionalLayout()) ?
      executeForTxLayout(collection) :
      executeForGVELayout(collection);
  }

  /**
   * Executes the operator for collections based on
   * {@link org.gradoop.flink.model.impl.layouts.gve.GVELayout}
   *
   * @param collection graph collection
   * @return result graph collection
   */
  GraphCollection executeForGVELayout(GraphCollection collection);

  /**
   * Executes the operator for collections based on
   * {@link org.gradoop.flink.model.impl.layouts.transactional.TxCollectionLayout}
   *
   * @param collection graph collection
   * @return result graph collection
   */
  GraphCollection executeForTxLayout(GraphCollection collection);
}
