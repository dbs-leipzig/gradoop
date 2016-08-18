package org.gradoop.flink.algorithms.fsm.gspan.miners;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.gspan.api.GSpanMiner;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.GSpanGraph;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Created by peet on 17.08.16.
 */
public class GSpanIterative implements GSpanMiner {

  @Override
  public DataSet<WithCount<CompressedDFSCode>> mine(DataSet<GSpanGraph> graphs,
    DataSet<Integer> minFrequency, FSMConfig fsmConfig) {
    return graphs
      .mapPartition(new CacheBasedGSpan(fsmConfig));
  }

  @Override
  public void setExecutionEnvironment(ExecutionEnvironment env) {

  }
}
