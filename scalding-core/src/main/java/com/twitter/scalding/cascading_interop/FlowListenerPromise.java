/*
Copyright 2014 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.twitter.scalding.cascading_interop;

import cascading.flow.FlowListener;
import cascading.flow.FlowException;
import cascading.flow.FlowStepListener;
import cascading.flow.FlowStep;
import cascading.flow.Flow;
import cascading.stats.CascadingStats;

import scala.concurrent.Promise$;
import scala.concurrent.Promise;
import scala.concurrent.Future;

import com.twitter.scalding.FatalExecutionError;

/*
 * The cascading API uses a raw type here which is difficult to
 * deal with in scala
 */
public class FlowListenerPromise {
  public static class FlowStopException extends FatalExecutionError {
    public FlowStopException(String message) {
      super(message);
    }
  }

  /*
   * This starts the flow and applies a mapping function fn in
   * the same thread that completion happens
   */
  public static <Config, T> Future<T> start(Flow<Config> flow, final scala.Function1<Flow<Config>, T> fn) {
    final Promise<T> result = Promise$.MODULE$.<T>apply();
    flow.addListener(new FlowListener() {
      public void onStarting(Flow f) { } // ignore
      public void onStopping(Flow f) { // in case of runtime exception cascading call onStopping
        result.tryFailure(new FlowStopException("Flow was stopped"));
      }
      public void onCompleted(Flow f) {
        // This is always called, but onThrowable is called first
        if(!result.isCompleted()) {
          if (f.getFlowStats().isSuccessful()) {
            // we use the above rather than trySuccess to avoid calling fn twice
            try {
              T toPut = (T) fn.apply(f);
              result.success(toPut);
            } catch (Throwable t) {
              result.tryFailure(t);
            }
          } else {
            result.tryFailure(new Exception("Flow was not successfully finished"));
          }
        }
      }
      public boolean onThrowable(Flow f, Throwable t) {
        result.tryFailure(t);
        // The exception is handled by the owner of the promise and should not be rethrown
        return true;
      }
    });
    flow.addStepListener(new FlowStepListener() {
      public void onStepStarting(FlowStep flowStep) { } // ignore
      public void onStepRunning(FlowStep flowStep) { } // ignore
      public void onStepCompleted(FlowStep flowStep) { } // ignore
      public void onStepStopping(FlowStep f) { result.tryFailure(new FlowStopException("Flow step was stopped")); }
      public boolean onStepThrowable(FlowStep f, Throwable t) {
        if (t != null) {
          result.tryFailure(t);
        } else {
          result.tryFailure(new FlowException("Flow step failed: " + f.getName()));
        }
        // The exception is handled by the owner of the promise and should not be rethrown
        return true;
      }
    });
    flow.start();
    return result.future();
  }
}
