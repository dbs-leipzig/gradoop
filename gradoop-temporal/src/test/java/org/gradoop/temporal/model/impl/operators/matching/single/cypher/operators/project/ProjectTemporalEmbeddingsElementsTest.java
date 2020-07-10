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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.project;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperatorTest;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;
import org.junit.Test;

import java.util.HashSet;

import static org.junit.Assert.assertEquals;

public class ProjectTemporalEmbeddingsElementsTest extends PhysicalTPGMOperatorTest {
  @Test
  public void testProjectTemporalEmbeddingsElements() throws Exception {
    HashSet<String> projectionVariables = new HashSet<>();
    projectionVariables.add("a");
    projectionVariables.add("b");

    EmbeddingTPGM emb1 = new EmbeddingTPGM();
    GradoopId aID = GradoopId.get();
    emb1.add(aID);
    GradoopId bID = GradoopId.get();
    emb1.add(bID);
    emb1.add(GradoopId.get());
    emb1.addTimeData(1L, 2L, 3L, 4L);

    EmbeddingTPGMMetaData metaIn = new EmbeddingTPGMMetaData();
    metaIn.setTimeColumn("a", 0);
    metaIn.setEntryColumn("a", EmbeddingMetaData.EntryType.VERTEX, 0);
    metaIn.setEntryColumn("b", EmbeddingMetaData.EntryType.VERTEX, 1);
    metaIn.setEntryColumn("c", EmbeddingMetaData.EntryType.VERTEX, 2);

    EmbeddingTPGMMetaData metaOut = new EmbeddingTPGMMetaData();
    metaOut.setTimeColumn("a", 0);
    metaOut.setEntryColumn("a", EmbeddingMetaData.EntryType.VERTEX, 1);
    metaOut.setEntryColumn("b", EmbeddingMetaData.EntryType.VERTEX, 0);
    //should be irrelevant
    metaOut.setEntryColumn("c", EmbeddingMetaData.EntryType.VERTEX, 2);

    DataSet<EmbeddingTPGM> data = getExecutionEnvironment().fromElements(emb1);

    DataSet<EmbeddingTPGM> result = new ProjectTemporalEmbeddingsElements(data,
      projectionVariables, metaIn, metaOut).evaluate();

    assertEquals(result.count(), 1);
    EmbeddingTPGM res = result.collect().get(0);
    assertEquals(res.getTimeData().length, 0);
    assertEquals(res.getPropertyData().length, 0);
    assertEquals(res.getId(0), bID);
    assertEquals(res.getId(1), aID);
    assertEquals(res.size(), 2);
  }

}
