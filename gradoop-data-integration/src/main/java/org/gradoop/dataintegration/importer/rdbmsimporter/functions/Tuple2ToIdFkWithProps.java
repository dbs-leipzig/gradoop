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
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.dataintegration.importer.rdbmsimporter.tuples.Fk1Fk2Props;
import org.gradoop.dataintegration.importer.rdbmsimporter.tuples.IdKeyTuple;

/**
 * Creates tuples of gradoop id (fk1), key value (fk2) and belonging properties
 */
@FunctionAnnotation.ForwardedFieldsFirst("1; 2")
@FunctionAnnotation.ForwardedFieldsSecond("0")
public class Tuple2ToIdFkWithProps implements
  MapFunction<Tuple2<Fk1Fk2Props, IdKeyTuple>, Tuple3<GradoopId, String, Properties>> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 1L;

  /**
   * Reuse tuple to avoid redundant instantiations.
   */
  private Tuple3<GradoopId, String, Properties> reuseTuple;

  /**
   * Instantiate the reuse tuple.
   */
  Tuple2ToIdFkWithProps() {
    this.reuseTuple = new Tuple3<>();
  }

  @Override
  public Tuple3<GradoopId, String, Properties> map(Tuple2<Fk1Fk2Props, IdKeyTuple> value) {
    reuseTuple.f0 = value.f1.f0;
    reuseTuple.f1 = value.f0.f1;
    reuseTuple.f2 = value.f0.f2;

    return reuseTuple;
  }
}
