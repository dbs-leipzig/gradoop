package org.gradoop.benchmark.nesting.serializers;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;

public  class CountHelper<T> extends RichOutputFormat<T> {
    private static final long serialVersionUID = 1L;
    private final String id;
    private long counter;

    public CountHelper(String id) {
      this.id = id;
      this.counter = 0L;
    }

    @Override
    public void configure(Configuration configuration) {

    }

    public void open(int taskNumber, int numTasks) {
    }

    public void writeRecord(T record) {
      ++this.counter;
    }

    public void close() {
      this.getRuntimeContext().getLongCounter(this.id).add(this.counter);
    }
}