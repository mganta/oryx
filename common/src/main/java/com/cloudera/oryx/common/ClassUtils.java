/*
 * Copyright (c) 2013, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.oryx.common;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

/**
 * {@link Class}-related utility methods.
 * 
 * @author Sean Owen
 */
public final class ClassUtils {

  private static final Class<?>[] NO_TYPES = new Class<?>[0];
  private static final Object[] NO_ARGS = new Object[0];

  private ClassUtils() {
  }

  /**
   * Like {@link #loadInstanceOf(String, Class)} where the reference returned is of the same type as
   * the class being loaded -- not any supertype.
   */
  public static <T> T loadInstanceOf(Class<T> clazz) {
    return loadInstanceOf(clazz.getName(), clazz);
  }

  /**
   * Like {@link #loadInstanceOf(String, Class, Class[], Object[])} where the reference returned is
   * of the same type as the class being loaded -- not any supertype.
   */
  public static <T> T loadInstanceOf(Class<T> clazz, Class<?>[] constructorTypes, Object[] constructorArgs) {
    return loadInstanceOf(clazz.getName(), clazz, constructorTypes, constructorArgs);
  }

  /**
   * Like {@link #loadInstanceOf(String, Class, Class[], Object[])} for no-arg constructors.
   */
  public static <T> T loadInstanceOf(String implClassName, Class<T> superClass) {
    return loadInstanceOf(implClassName, superClass, NO_TYPES, NO_ARGS);
  }

  /**
   * Loads and instantiates a named implementation class, a subclass of a given supertype,
   * whose constructor takes the given arguments.
   *
   * @param implClassName implementation class name
   * @param superClass superclass or interface that the implementation extends
   * @param constructorTypes argument types of constructor to use
   * @param constructorArgs actual constructor arguments
   * @return instance of {@code implClassName}
   */
  public static <T> T loadInstanceOf(String implClassName,
                                     Class<T> superClass,
                                     Class<?>[] constructorTypes,
                                     Object[] constructorArgs) {
    return doLoadInstanceOf(implClassName,
                            superClass,
                            constructorTypes,
                            constructorArgs,
                            ClassUtils.class.getClassLoader());
  }

  private static <T> Class<? extends T> doLoadClass(String implClassName,
                                                    Class<T> superClass,
                                                    ClassLoader classLoader) {
    try {
      return Class.forName(implClassName, true, classLoader).asSubclass(superClass);
    } catch (ClassNotFoundException cnfe) {
      throw new IllegalStateException("No valid " + superClass + " binding exists", cnfe);
    }
  }

  private static <T> T doLoadInstanceOf(String implClassName,
                                        Class<T> superClass,
                                        Class<?>[] constructorTypes,
                                        Object[] constructorArgs,
                                        ClassLoader classLoader) {
    try {
      Class<? extends T> configClass = doLoadClass(implClassName, superClass, classLoader);
      Constructor<? extends T> constructor = configClass.getConstructor(constructorTypes);
      return constructor.newInstance(constructorArgs);
    } catch (NoSuchMethodException | IllegalAccessException | InstantiationException nsme) {
      throw new IllegalStateException("No valid " + superClass + " binding exists", nsme);
    } catch (InvocationTargetException ite) {
      throw new IllegalStateException("Could not instantiate " + superClass + " due to exception", ite.getCause());
    }
  }

  /**
   * @param implClassName class name to text
   * @return {@code true} if the class exists in the JVM and can be loaded
   */
  public static boolean classExists(String implClassName) {
    try {
      Class.forName(implClassName);
      return true;
    } catch (ClassNotFoundException ignored) {
      return false;
    }
  }

  /**
   * @param clazz the class containing the field to load
   * @param fieldName the field name
   * @return the named {@link Field}, made accessible, from the given {@link Class}
   */
  public static Field loadField(Class<?> clazz, String fieldName) {
    Field field;
    try {
      field = clazz.getDeclaredField(fieldName);
    } catch (NoSuchFieldException nsfe) {
      throw new IllegalStateException("Can't access " + clazz.getSimpleName() + '.' + fieldName, nsfe);
    }
    field.setAccessible(true);
    return field;
  }

}
