package org.gradoop.flink.model.impl.operators.nest.nestingwithdisjunctive;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.operators.GraphGraphCollectionToGraphOperator;
import org.gradoop.flink.model.impl.operators.nest.AbstractPaperTest;
import org.gradoop.flink.model.impl.operators.nest.NestingWithDisjunctive;

public class PaperTest extends AbstractPaperTest {
  @Override
  protected GraphGraphCollectionToGraphOperator op() {
    return new NestingWithDisjunctive(GradoopId.get());
  }
}
