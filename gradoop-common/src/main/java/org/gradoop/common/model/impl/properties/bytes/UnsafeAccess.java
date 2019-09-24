/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

public final class UnsafeAccess {

  //private static final Logger LOG = Logger.getLogger(UnsafeAvailChecker.class);

  public static final Unsafe theUnsafe;

  /** The offset to the first element in a byte array. */
  public static final int BYTE_ARRAY_BASE_OFFSET;

  static {
    theUnsafe = (Unsafe) AccessController.doPrivileged(new PrivilegedAction<Object>() {
      @Override
      public Object run() {
        try {
          Field f = Unsafe.class.getDeclaredField("theUnsafe");
          f.setAccessible(true);
          return f.get(null);
        } catch (Throwable e) {
          //LOG.warn("sun.misc.Unsafe is not accessible", e);
          throw new RuntimeException(e);
        }
        //return null;
      }
    });

    if(theUnsafe != null){
      BYTE_ARRAY_BASE_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);
    } else{
      BYTE_ARRAY_BASE_OFFSET = -1;
    }
  }

  private UnsafeAccess(){}
}
