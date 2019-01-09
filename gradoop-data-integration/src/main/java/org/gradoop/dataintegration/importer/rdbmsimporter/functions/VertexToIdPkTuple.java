/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.dataintegration.importer.rdbmsimporter.tuples.IdKeyTuple;

import java.util.Objects;

import static org.gradoop.dataintegration.importer.rdbmsimporter.constants.RdbmsConstants.PK_ID;

/**
 * Creates tuples of gradoop id, primary key name from vertices
 */
public class VertexToIdPkTuple implements MapFunction<Vertex, IdKeyTuple> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 1L;

  /**
   * Reuse tuple to avoid needless instantiation.
   */
  private IdKeyTuple reuseTuple;

  /**
   * Instantiate the reuse tuple.
   */
  VertexToIdPkTuple() {
    this.reuseTuple = new IdKeyTuple();
  }

  @Override
  public IdKeyTuple map(Vertex v) {
    reuseTuple.f0 = v.getId();
    reuseTuple.f1 = Objects.requireNonNull(v.getProperties()).get(PK_ID).toString();

    return reuseTuple;
  }
}
