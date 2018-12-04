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

package org.gradoop.dataintegration.importer.rdbmsimporter.functions;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.dataintegration.importer.rdbmsimporter.metadata.TableToEdge;
import org.gradoop.dataintegration.importer.rdbmsimporter.tuples.LabelIdKeyTuple;

import java.util.List;

import static org.gradoop.dataintegration.importer.rdbmsimporter.constants.RdbmsConstants.BROADCAST_VARIABLE;

/**
 * Collects label,gradoop id, foreign key value of vertices
 */
public class VerticesToFkTable extends RichFlatMapFunction<TableToEdge, LabelIdKeyTuple> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 1L;

  /**
   * List of converted vertices
   */
  private List<Vertex> vertices;

  @Override
  public void flatMap(TableToEdge table, Collector<LabelIdKeyTuple> out) throws Exception {
    String label = table.getStartAttribute().f0;
    GradoopId id;
    String key;

    for (Vertex v : vertices) {
      if (v.getLabel().equals(table.getStartTableName())) {
        id = v.getId();
        key = v.getProperties().get(table.getStartAttribute().f0).toString();
        out.collect(new LabelIdKeyTuple(label, id, key));
      }
    }
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    this.vertices =
      getRuntimeContext().getBroadcastVariable(BROADCAST_VARIABLE);
  }
}
