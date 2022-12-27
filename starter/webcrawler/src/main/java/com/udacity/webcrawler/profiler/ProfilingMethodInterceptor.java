package com.udacity.webcrawler.profiler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.stream.StreamSupport;

/**
 * A method interceptor that checks whether {@link Method}s are annotated with the {@link Profiled}
 * annotation. If they are, the method interceptor records how long the method invocation took.
 */
final class ProfilingMethodInterceptor implements InvocationHandler {

  private final Clock clock;
  private final Object targetObject;
  private final ProfilingState state;


  // TODO: You will need to add more instance fields and constructor arguments to this class.
  public ProfilingMethodInterceptor(Clock clock, Object targetObject, ProfilingState state) {
    this.clock = clock;
    this.targetObject = targetObject;
    this.state = state;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    // TODO: This method interceptor should inspect the called method to see if it is a profiled
    //       method. For profiled methods, the interceptor should record the start time, then
    //       invoke the method using the object that is being profiled. Finally, for profiled
    //       methods, the interceptor should record how long the method call took, using the
    //       ProfilingState methods.
    Instant start = Instant.now(clock);
    Object resultObj = null;
    try
    {
      resultObj = method.invoke(targetObject,args);
    }
    catch (InvocationTargetException ex)
    {
      throw ex.getTargetException();
    }
    catch (IllegalAccessException ex)
    {
      throw new RuntimeException(ex);
    }
    finally {
      if(method.getDeclaringClass().equals(Object.class) || method.getAnnotation(Profiled.class) != null) {
        state.record(targetObject.getClass(), method, Duration.between(start, clock.instant()));
      }
    }
    return resultObj;
  }
}
