/*
 * Copyright Terracotta, Inc.
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

package org.ehcache.clustered.common.internal.messages;

import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.exceptions.UnknownClusterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.StructArrayDecoder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructArrayEncoder;
import org.terracotta.runnel.encoding.StructEncoder;
import org.terracotta.runnel.encoding.StructEncoderFunction;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

final class ExceptionCodec {

  private ExceptionCodec() {
    //no instances please
  }

  public static final StructEncoderFunction<ClusterException> EXCEPTION_ENCODER_FUNCTION = new StructEncoderFunction<ClusterException>() {
    @Override
    public void encode(StructEncoder<?> encoder, ClusterException exception) {
      ExceptionCodec.encode(encoder, exception);
    }
  };

  private static final Logger LOGGER = LoggerFactory.getLogger(ExceptionCodec.class);

  private static final String DECLARING_CLASS_FIELD = "declaringClass";
  private static final String METHOD_NAME_FIELD = "methodName";
  private static final String FILE_NAME_FIELD = "fileName";
  private static final String LINE_NUM_FIELD = "lineNumber";
  private static final String FQCN_FIELD = "fqcn";
  private static final String MESSAGE_FIELD = "message";
  private static final String STACKTRACE_ELEMENTS_FIELD = "stacktraceElements";

  private static final Struct STE_STRUCT = StructBuilder.newStructBuilder()
      .string(DECLARING_CLASS_FIELD, 10)
      .string(METHOD_NAME_FIELD, 20)
      .string(FILE_NAME_FIELD, 30)
      .int32(LINE_NUM_FIELD, 40)
      .build();

  static final Struct EXCEPTION_STRUCT = StructBuilder.newStructBuilder()
      .string(FQCN_FIELD, 10)
      .string(MESSAGE_FIELD, 20)
      .structs(STACKTRACE_ELEMENTS_FIELD, 30, STE_STRUCT)
      .build();

  public static void encode(StructEncoder<?> encoder, ClusterException exception) {
    encoder.string(FQCN_FIELD, exception.getClass().getCanonicalName());
    encoder.string(MESSAGE_FIELD, exception.getMessage());
    StructArrayEncoder<?> arrayEncoder = encoder.structs(STACKTRACE_ELEMENTS_FIELD);
    for (StackTraceElement stackTraceElement : exception.getStackTrace()) {
      StructEncoder<?> element = arrayEncoder.add();
      element.string(DECLARING_CLASS_FIELD, stackTraceElement.getClassName());
      element.string(METHOD_NAME_FIELD, stackTraceElement.getMethodName());
      if (stackTraceElement.getFileName() != null) {
        element.string(FILE_NAME_FIELD, stackTraceElement.getFileName());
      }
      element.int32(LINE_NUM_FIELD, stackTraceElement.getLineNumber());
      element.end();
    }
    arrayEncoder.end();
  }

  public static ClusterException decode(StructDecoder<StructDecoder<Void>> decoder) {
    String exceptionClassName = decoder.string(FQCN_FIELD);
    String message = decoder.string(MESSAGE_FIELD);
    StructArrayDecoder<StructDecoder<StructDecoder<Void>>> arrayDecoder = decoder.structs(STACKTRACE_ELEMENTS_FIELD);
    StackTraceElement[] stackTraceElements = new StackTraceElement[arrayDecoder.length()];
    for (int i = 0; i < arrayDecoder.length(); i++) {
      StructDecoder<?> element = arrayDecoder.next();
      stackTraceElements[i] = new StackTraceElement(
        element.string(DECLARING_CLASS_FIELD),
        element.string(METHOD_NAME_FIELD),
        element.string(FILE_NAME_FIELD),
        element.int32(LINE_NUM_FIELD));
      element.end();
    }
    arrayDecoder.end();
    Class clazz = null;
    ClusterException exception = null;
    try {
      clazz = Class.forName(exceptionClassName);
    } catch (ClassNotFoundException e) {
      LOGGER.error("Exception type not found", e);
    }
    exception = getClusterException(message, clazz);
    if (exception == null) {
      exception = new UnknownClusterException(message);
    }
    exception.setStackTrace(stackTraceElements);
    return exception;
  }

  @SuppressWarnings("unchecked")
  private static ClusterException getClusterException(String message, Class clazz) {
    ClusterException exception = null;
    if (clazz != null) {
      try {
        Constructor declaredConstructor = clazz.getDeclaredConstructor(String.class);
        exception = (ClusterException)declaredConstructor.newInstance(message);
      } catch (NoSuchMethodException e) {
        LOGGER.error("Failed to instantiate exception object.", e);
      } catch (IllegalAccessException e) {
        LOGGER.error("Failed to instantiate exception object.", e);
      } catch (InstantiationException e) {
        LOGGER.error("Failed to instantiate exception object.", e);
      } catch (InvocationTargetException e) {
        LOGGER.error("Failed to instantiate exception object.", e);
      }
    }
    return exception;
  }

}
