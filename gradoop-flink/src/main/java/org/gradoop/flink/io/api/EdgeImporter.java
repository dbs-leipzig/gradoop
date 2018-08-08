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
package org.gradoop.flink.io.api;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;

/**
 * Base interface to import external edges into a DataSet of EPGM Edges.
 * It prepared the edges for execution of the DataSource.
 * @param <K> Type of the edge identifier type
 */
public interface EdgeImporter<K extends Comparable<K>> {

 /**
  * The import method. Takes for each edge the elements of the external edge and
  * import it into EPGM. Put all imported edges into a DataSet.
  * @return DataSet of all imported edges.
  */
  DataSet<ImportEdge<K>> importEdge();
}
