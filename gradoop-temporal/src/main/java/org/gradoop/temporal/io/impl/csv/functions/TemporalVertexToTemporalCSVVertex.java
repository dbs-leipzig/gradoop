/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.io.impl.csv.functions;

import org.gradoop.flink.io.api.metadata.MetaDataSource;
import org.gradoop.flink.io.impl.csv.CSVConstants;
import org.gradoop.flink.io.impl.csv.functions.StringEscaper;
import org.gradoop.temporal.io.impl.csv.tuples.TemporalCSVVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

/**
 * Converts an {@link TemporalVertex} into a CSV representation.
 */
public class TemporalVertexToTemporalCSVVertex
  extends TemporalElementToCSV<TemporalVertex, TemporalCSVVertex> {

  /**
   * Reduce object instantiations.
   */
  private final TemporalCSVVertex csvVertex = new TemporalCSVVertex();

  @Override
  public TemporalCSVVertex map(TemporalVertex temporalVertex) {
    csvVertex.setId(temporalVertex.getId().toString());
    csvVertex.setGradoopIds(collectionToCsvString(temporalVertex.getGraphIds()));
    csvVertex.setLabel(StringEscaper.escape(temporalVertex.getLabel(), CSVConstants.ESCAPED_CHARACTERS));
    csvVertex.setProperties(getPropertyString(temporalVertex, MetaDataSource.VERTEX_TYPE));
    csvVertex.setTemporalData(getTemporalDataString(temporalVertex.getTransactionTime(),
      temporalVertex.getValidTime()));
    return csvVertex;
  }
}
