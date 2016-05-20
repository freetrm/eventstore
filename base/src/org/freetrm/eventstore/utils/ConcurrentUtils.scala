package org.freetrm.eventstore.utils

import java.util.concurrent._

object ConcurrentUtils {

  /**
   * This will put the key into the map if is it not already present. Then it will call the
   * method f and return its result.
   * If k is already in the map the previous value will be returned.
   *
   * The boolean part of the return indicates if the key was already in the map. 
   *
   * Taken from Java Concurrency in Practice
   */
  def putIfAbsentWithCheck[K, V](map: ConcurrentMap[K, FutureTask[V]], k: K, f: => V): (V, Boolean) = {
    val task = new FutureTask[V](new Callable[V]() {
      def call = f
    })
    var hit = true
    var actualTask = map.putIfAbsent(k, task)
    if (actualTask == null) {
      hit = false
      actualTask = task
      task.run()
    }
    try {
      (actualTask.get(), hit)
    } catch {
      case e: ExecutionException => throw e.getCause
    }
  }

  def putIfAbsent[K, V](map: ConcurrentMap[K, FutureTask[V]], k: K, f: => V): V = {
    putIfAbsentWithCheck(map, k, f)._1
  }

}

