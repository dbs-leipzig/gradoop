package org.gradoop.common.model.api.strategies;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.gradoop.common.model.impl.id.GradoopId;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface PropertyValueStrategy<T> {

  boolean write(T value, DataOutputView outputView) throws IOException;

  T read(DataInputView inputView, byte typeByte) throws IOException;

  int compare(T value, Object other);

  boolean is(Object value);

  Class<T> getType();

  T get(byte[] bytes);

  Byte getRawType();

  byte[] getRawBytes(T value);

}
