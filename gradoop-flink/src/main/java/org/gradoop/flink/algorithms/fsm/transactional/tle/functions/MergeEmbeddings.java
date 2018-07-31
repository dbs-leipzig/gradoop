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
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.SubgraphEmbeddings;

import java.util.Iterator;

/**
 * (graph, subgraph, embeddings),.. => (graph, "", embeddings)
 *
 * @param <SE>
 */
public class MergeEmbeddings<SE extends SubgraphEmbeddings>
  implements GroupReduceFunction<SE, SE> {

  @Override
  public void reduce(Iterable<SE> iterable,
    Collector<SE> collector) throws Exception {

    Iterator<SE> iterator = iterable.iterator();

    SE out = iterator.next();
    out.setCanonicalLabel("");

    while (iterator.hasNext()) {
      out.getEmbeddings().addAll(iterator.next().getEmbeddings());
    }

    collector.collect(out);
  }
}
