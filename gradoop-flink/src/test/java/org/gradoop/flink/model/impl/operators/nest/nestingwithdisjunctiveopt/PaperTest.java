package org.gradoop.flink.model.impl.operators.nest.nestingwithdisjunctiveopt;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.operators.GraphGraphCollectionToGraphOperator;
import org.gradoop.flink.model.impl.operators.nest.AbstractPaperTest;
import org.gradoop.flink.model.impl.operators.nest.NestingWithDisjunctiveOpt;

public class PaperTest extends AbstractPaperTest {
  @Override
  protected GraphGraphCollectionToGraphOperator op() {
    return new NestingWithDisjunctiveOpt(GradoopId.get());
  }
}
