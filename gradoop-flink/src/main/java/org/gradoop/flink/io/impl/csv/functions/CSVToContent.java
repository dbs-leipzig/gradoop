package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;
import org.gradoop.flink.io.impl.csv.pojos.CsvExtension;

import java.util.List;


public class CSVToContent
  implements GroupReduceFunction<String, Tuple2<CsvExtension, List<String>>> {

  private CsvExtension csv;

  public CSVToContent(CsvExtension csv) {
    this.csv = csv;
  }

  @Override
  public void reduce(Iterable<String> iterable,
    Collector<Tuple2<CsvExtension, List<String>>> collector) throws Exception {
    collector.collect(
      new Tuple2<CsvExtension, List<String>>(csv, Lists.newArrayList(iterable)));
  }
}
