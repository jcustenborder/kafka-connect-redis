/**
 * Copyright (c) 2014-2020 project44
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.redis;

import java.util.Arrays;

/**
 * Wrapper around a byte[] so that it can be used as a key in HashMaps to prevent
 * creating a ton of strings or some other less efficient workaround
 */
public class ByteString {
  private final byte[] value;

  private ByteString(final byte[] value) {
    this.value = value;
  }

  public byte[] getBytes() {
    return this.value;
  }

  public static ByteString of(final byte[] value) {
    return new ByteString(value);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || (obj.getClass() != this.getClass())) {
      return false;
    }

    final ByteString byteString = (ByteString) obj;
    return Arrays.equals(this.value, byteString.getBytes());
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.value);
  }
}
