package org.gradoop.flink.model.impl.operators.nest.nestingwithcompactdisjunctive;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.operators.GraphGraphCollectionToGraphOperator;
import org.gradoop.flink.model.impl.operators.nest.AbstractPaperTest;
import org.gradoop.flink.model.impl.operators.nest.NestingWithDisjunctive;
import org.gradoop.flink.model.impl.operators.nest.NestingWithDisjunctiveCompact;

public class PaperTest extends AbstractPaperTest {
  @Override
  protected GraphGraphCollectionToGraphOperator op() {
    return new NestingWithDisjunctiveCompact(GradoopId.get());
  }
}
