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
package org.gradoop.flink.model.api.layouts;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

/**
 * A graph collection layout defines the Flink internal (DataSet) representation of a
 * {@link org.gradoop.flink.model.api.epgm.GraphCollection}.
 */
public interface GraphCollectionLayout extends Layout {

  /**
   * True, if the layout is based on three separate datasets.
   *
   * @return true, iff layout based on three separate datasets.
   */
  boolean isGVELayout();

  /**
   * True, if the layout is based on separate datasets separated by graph, vertex and edge labels.
   *
   * @return true, iff layout is based on label-separated datasets
   */
  boolean isIndexedGVELayout();

  /**
   * True, if the layout is based on a transactional data representation.
   *
   * @return true, iff layout based on a transactional representation
   */
  boolean isTransactionalLayout();
  /**
   * Returns the graph heads associated with the logical graphs in that
   * collection.
   *
   * @return graph heads
   */
  DataSet<GraphHead> getGraphHeads();

  /**
   * Returns the graph heads associated with the logical graphs in that
   * collection filtered by label.
   *
   * @param label graph head label
   * @return graph heads
   */
  DataSet<GraphHead> getGraphHeadsByLabel(String label);

  /**
   * Returns the graph collection represented as graph transactions. Each transactions represents
   * a single logical graph with all its data.
   *
   * @return graph transactions
   */
  DataSet<GraphTransaction> getGraphTransactions();
}
