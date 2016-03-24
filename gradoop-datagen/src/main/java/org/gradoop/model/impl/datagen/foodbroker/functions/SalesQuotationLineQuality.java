package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.datagen.foodbroker.model.TransactionalDataObject;
import org.gradoop.model.impl.id.GradoopId;


public class SalesQuotationLineQuality implements
  GroupReduceFunction<TransactionalDataObject, Tuple2<GradoopId, Short>> {

  @Override
  public void reduce(Iterable<TransactionalDataObject> iterable,
    Collector<Tuple2<GradoopId, Short>> collector) throws Exception {

    Short qualitySum = (short) 0;
    Boolean first = true;
    GradoopId quoationId = null;

    for(TransactionalDataObject line : iterable) {
      if(first) {
        quoationId =
          line.getReferences().get(SalesQuotationLine.PART_OF);
        first = false;
      }

      qualitySum = (short)
        (qualitySum + line.getQualities().get(SalesQuotationLine.CONTAINS));
    }


    collector.collect(new Tuple2<>(quoationId, qualitySum));
  }
}
