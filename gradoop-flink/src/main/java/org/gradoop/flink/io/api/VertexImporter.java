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
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

/**
 * Base interface to import external vertices into a DataSet of EPGM Vertices.
 * The vertices will be prepared for execution of the DataSource.
 *
 * @param <K> Type of the vertex identifier type
 */
public interface VertexImporter<K extends Comparable<K>> {

 /**
  * The import method. Takes for each vertex the elements of the external vertex and
  * import it into EPGM. Put all imported vertices into a DataSet.
  * @return DataSet of all imported vertices.
  */
  DataSet<ImportVertex<K>> getImportVertex();
}
