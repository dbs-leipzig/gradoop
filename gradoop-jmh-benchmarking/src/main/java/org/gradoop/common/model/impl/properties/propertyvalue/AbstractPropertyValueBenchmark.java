package org.gradoop.common.model.impl.properties.propertyvalue;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import java.util.concurrent.TimeUnit;

@Warmup(time = 1, timeUnit = TimeUnit.SECONDS, iterations = 5)
@Measurement(time = 1, timeUnit = TimeUnit.SECONDS, iterations = 5)
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@Fork(1)
public abstract class AbstractPropertyValueBenchmark {

  public abstract void setup();

  public abstract PropertyValue create();

  public abstract void set();

  public abstract Boolean is();

  public abstract Object get();

  public abstract void setObject();

  public abstract Class<?> getType();

  /**
   * Wrapper around benchmark specific setup method.
   */
  @Setup
  public void runSetup() {
    setup();
  }

  /**
   * Wrapper around benchmark specific PropertyValue.create() method.
   * @return PropertyValue to prevent dead code elimination by JVM.
   */
  @Benchmark
  public PropertyValue benchmarkCreate() {
    return create();
  }

  /**
   * Wrapper around benchmark specific type check method.
   * @return Boolean to prevent dead code elimination by JVM.
   */
  @Benchmark
  public Boolean benchmarkIs() {
    return is();
  }

  /**
   * Wrapper around benchmark specific data retrieval method.
   * @return Object to prevent dead code elimination by JVM.
   */
  @Benchmark
  public Object benchmarkGet() {
    return get();
  }

  /**
   * Wrapper around benchmark specific PropertyValue.setObject() method.
   */
  @Benchmark
  public void benchmarkSetObject() {
    setObject();
  }

  /**
   * Wrapper around benchmark specific PropertyValue.getType() method.
   * @return Class of used data type to prevent dead code elimination by JVM.
   */
  @Benchmark
  public Class<?> benchmarkGetType() {
    return getType();
  }

  /**
   * Baseline method to check whether benchmarking results are credible.
   */
  @Benchmark
  public void baseline() {
    /* do nothing */
  }



}
