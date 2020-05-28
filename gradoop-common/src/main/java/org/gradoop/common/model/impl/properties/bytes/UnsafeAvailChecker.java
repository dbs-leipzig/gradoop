/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.common.model.impl.properties.bytes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * This implementation reuses much of the code of HBase's UnsafeAvailChecker
 * (org.apache.hadoop.hbase.util.UnsafeAvailChecker).
 * This can be found in org.apache.hbase:hbase-common
 * Much of the code is copied directly or has only small changes.
 */
class UnsafeAvailChecker {

  /**
   * Unsafe class name
   */
  private static final String CLASS_NAME = "sun.misc.Unsafe";

  /**
   * true, when Unsafe available and system has unaligned access capability
   */
  private static boolean UNALIGNED = false;

  /**
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(UnsafeAvailChecker.class);

  static {
    boolean avail = AccessController.doPrivileged(new PrivilegedAction<Boolean>() {
      @Override
      public Boolean run() {
        try {
          Class<?> clazz = Class.forName(CLASS_NAME);
          Field f = clazz.getDeclaredField("theUnsafe");
          f.setAccessible(true);
          return f.get(null) != null;
        } catch (ReflectiveOperationException e) {
          LOG.warn("sun.misc.Unsafe is not available/accessible");
          return false;
        }
      }
    });
    // When Unsafe itself is not available/accessible consider unaligned as false.
    if (avail) {
      String arch = System.getProperty("os.arch");
      if ("ppc64".equals(arch) || "ppc64le".equals(arch)) {
        // java.nio.Bits.unaligned() wrongly returns false on ppc (JDK-8165231),
        UNALIGNED = true;
      } else {
        try {
          // Using java.nio.Bits#unaligned() to check for unaligned-access capability
          Class<?> clazz = Class.forName("java.nio.Bits");
          Method m = clazz.getDeclaredMethod("unaligned");
          m.setAccessible(true);
          UNALIGNED = (Boolean) m.invoke(null);
        } catch (ReflectiveOperationException e) {
          LOG.warn("java.nio.Bits#unaligned() check failed. Unsafe based read/write of primitive types " +
            "won't be used", e);
        }
      }
    }
  }

  /**
   * Private constructor to avoid instantiation
   */
  private UnsafeAvailChecker() {
  }

  /**
   * True when running JVM is having sun's Unsafe package available in it and underlying
   * system having unaligned-access capability.
   *
   * @return true, if unaligned
   */
  static boolean unaligned() {
    return UNALIGNED;
  }
}
