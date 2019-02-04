/*
 * Scala (https://www.scala-lang.org)
 *
 * Copyright EPFL and Lightbend, Inc.
 *
 * Licensed under Apache License 2.0
 * (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 */

package scala.concurrent.impl
import scala.concurrent.{ Batchable, ExecutionContext, CanAwait, TimeoutException, ExecutionException, Future, OnCompleteRunnable }
import Future.InternalCallbackExecutor
import scala.concurrent.duration.Duration
import scala.annotation.{ tailrec, switch }
import scala.util.control.{ NonFatal, ControlThrowable }
import scala.util.{ Try, Success, Failure }
import scala.runtime.NonLocalReturnControl
import java.util.concurrent.locks.AbstractQueuedSynchronizer
import java.util.concurrent.atomic.{ AtomicReference, AtomicBoolean }
import java.util.Objects.requireNonNull

/**
  * Latch used to implement waiting on a DefaultPromise's result.
  *
  * Inspired by: http://gee.cs.oswego.edu/cgi-bin/viewcvs.cgi/jsr166/src/main/java/util/concurrent/locks/AbstractQueuedSynchronizer.java
  * Written by Doug Lea with assistance from members of JCP JSR-166
  * Expert Group and released to the public domain, as explained at
  * http://creativecommons.org/publicdomain/zero/1.0/
  */
private[impl] final class CompletionLatch[T] extends AbstractQueuedSynchronizer with (Try[T] => Unit) {
  //@volatie not needed since we use acquire/release
  /*@volatile*/ private[this] var _result: Try[T] = null
  final def result: Try[T] = _result
  override protected def tryAcquireShared(ignored: Int): Int = if (getState != 0) 1 else -1
  override protected def tryReleaseShared(ignore: Int): Boolean = {
    setState(1)
    true
  }
  override def apply(value: Try[T]): Unit = {
    _result = value // This line MUST go before releaseShared
    releaseShared(1)
  }
}

private[concurrent] object Promise {
    /**
     * The process of "resolving" a Try is to validate that it only contains
     * those values which makes sense in the context of Futures.
     **/
    // requireNonNull is paramount to guard against null completions
    private[this] final def resolve[T](value: Try[T]): Try[T] =
      if (requireNonNull(value).isInstanceOf[Success[T]]) value
      else {
        val t = value.asInstanceOf[Failure[T]].exception
        if (t.isInstanceOf[ControlThrowable] || t.isInstanceOf[InterruptedException] || t.isInstanceOf[Error]) {
          if (t.isInstanceOf[NonLocalReturnControl[T @unchecked]])
            Success(t.asInstanceOf[NonLocalReturnControl[T]].value)
          else
            Failure(new ExecutionException("Boxed Exception", t))
        } else value
      }

  // Left non-final to enable addition of extra fields by Java/Scala converters in scala-java8-compat.
  class DefaultPromise[T] private[this] (initial: AnyRef) extends AtomicReference[AnyRef](initial) with scala.concurrent.Promise[T] with scala.concurrent.Future[T] with (Try[T] => Unit) {
    /**
     * Constructs a new, completed, Promise.
     */
    final def this(result: Try[T]) = this(resolve(result): AnyRef)

    /**
     * Constructs a new, un-completed, Promise.
     */
    final def this() = this(Noop: AnyRef)

    /**
     * WARNING: the `resolved` value needs to have been pre-resolved using `resolve()`
     * INTERNAL API
     */
    override final def apply(resolved: Try[T]): Unit =
      tryComplete0(get(), resolved)

    /**
     * Returns the associated `Future` with this `Promise`
     */
    override final def future: Future[T] = this

    override final def transform[S](f: Try[T] => Try[S])(implicit executor: ExecutionContext): Future[S] =
      dispatchOrAddCallbacks(get(), new Transformation[T, S](Xform_transform, f, executor))

    override final def transformWith[S](f: Try[T] => Future[S])(implicit executor: ExecutionContext): Future[S] =
      dispatchOrAddCallbacks(get(), new Transformation[T, S](Xform_transformWith, f, executor))

    override final def foreach[U](f: T => U)(implicit executor: ExecutionContext): Unit = {
      val state = get()
      if (!state.isInstanceOf[Failure[T]]) dispatchOrAddCallbacks(state, new Transformation[T, Unit](Xform_foreach, f, executor))
    }

    override final def flatMap[S](f: T => Future[S])(implicit executor: ExecutionContext): Future[S] = {
      val state = get()
      if (!state.isInstanceOf[Failure[T]]) dispatchOrAddCallbacks(state, new Transformation[T, S](Xform_flatMap, f, executor))
      else this.asInstanceOf[Future[S]]
    }

    override final def map[S](f: T => S)(implicit executor: ExecutionContext): Future[S] = {
      val state = get()
      if (!state.isInstanceOf[Failure[T]]) dispatchOrAddCallbacks(state, new Transformation[T, S](Xform_map, f, executor))
      else this.asInstanceOf[Future[S]]
    }

    override final def filter(p: T => Boolean)(implicit executor: ExecutionContext): Future[T] = {
      val state = get()
      if (!state.isInstanceOf[Failure[T]]) dispatchOrAddCallbacks(state, new Transformation[T, T](Xform_filter, p, executor)) // Short-circuit if we get a Success
      else this
    }

    override final def collect[S](pf: PartialFunction[T, S])(implicit executor: ExecutionContext): Future[S] = {
      val state = get()
      if (!state.isInstanceOf[Failure[T]]) dispatchOrAddCallbacks(state, new Transformation[T, S](Xform_collect, pf, executor)) // Short-circuit if we get a Success
      else this.asInstanceOf[Future[S]]
    }

    override final def recoverWith[U >: T](pf: PartialFunction[Throwable, Future[U]])(implicit executor: ExecutionContext): Future[U] = {
      val state = get()
      if (!state.isInstanceOf[Success[T]]) dispatchOrAddCallbacks(state, new Transformation[T, U](Xform_recoverWith, pf, executor)) // Short-circuit if we get a Failure
      else this.asInstanceOf[Future[U]]
    }

    override final def recover[U >: T](pf: PartialFunction[Throwable, U])(implicit executor: ExecutionContext): Future[U] = {
      val state = get()
      if (!state.isInstanceOf[Success[T]]) dispatchOrAddCallbacks(state, new Transformation[T, U](Xform_recover, pf, executor)) // Short-circuit if we get a Failure
      else this.asInstanceOf[Future[U]]
    }

    override final def mapTo[S](implicit tag: scala.reflect.ClassTag[S]): Future[S] =
      if (!get().isInstanceOf[Failure[T]]) super[Future].mapTo[S](tag) // Short-circuit if we get a Success
      else this.asInstanceOf[Future[S]]


    override final def onComplete[U](func: Try[T] => U)(implicit executor: ExecutionContext): Unit =
      dispatchOrAddCallbacks(get(), new Transformation[T, Unit](Xform_onComplete, func, executor))

    override final def failed: Future[Throwable] =
      if (!get().isInstanceOf[Success[T]]) super.failed
      else Future.failedFailureFuture // Cached instance in case of already known success

    override final def toString: String = {
      val state = get()
      if (state.isInstanceOf[Try[T]]) "Future("+state+")"
      else /*if (state.isInstanceOf[Callbacks[T]]) */ "Future(<not completed>)"
    }

    private[this] final def tryAwait0(atMost: Duration): Try[T] =
      if (atMost ne Duration.Undefined) {
        val v = value0
        if (v ne null) v
        else {
          val r =
            if (atMost <= Duration.Zero) null
            else {
              val l = new CompletionLatch[T]()
              onComplete(l)(InternalCallbackExecutor)

              if (atMost.isFinite)
                l.tryAcquireSharedNanos(1, atMost.toNanos)
              else
                l.acquireSharedInterruptibly(1)

              l.result
            }
          if (r ne null) r
          else throw new TimeoutException("Future timed out after [" + atMost + "]")
        }
      } else throw new IllegalArgumentException("Cannot wait for Undefined duration of time")

    @throws(classOf[TimeoutException])
    @throws(classOf[InterruptedException])
    final def ready(atMost: Duration)(implicit permit: CanAwait): this.type = {
      tryAwait0(atMost)
      this
    }

    @throws(classOf[Exception])
    final def result(atMost: Duration)(implicit permit: CanAwait): T =
      tryAwait0(atMost).get // returns the value, or throws the contained exception

    override final def isCompleted: Boolean = value0 ne null

    override final def value: Option[Try[T]] = Option(value0)

    // returns null if not completed
    private final def value0: Try[T] = {
      val state = get()
      if (state.isInstanceOf[Try[T]]) state.asInstanceOf[Try[T]]
      else /*if (state.isInstanceOf[Callbacks[T]])*/ null
    }

    override final def tryComplete(value: Try[T]): Boolean = {
      val state = get()
      if (state.isInstanceOf[Try[T]]) false
      else tryComplete0(state, resolve(value))
    }

    @tailrec // WARNING: important that the supplied Try really is resolve():d
    private[Promise] final def tryComplete0(state: AnyRef, resolved: Try[T]): Boolean =
      if (state.isInstanceOf[Callbacks[T]]) {
        if (compareAndSet(state, resolved)) {
          if (state ne Noop) submitWithValue(state.asInstanceOf[Callbacks[T]], resolved)
          true
        } else tryComplete0(get(), resolved)
      } else /* if(state.isInstanceOf[Try[T]]) */ false

    override final def completeWith(other: Future[T]): this.type = {
      if ((other ne this) && other.isInstanceOf[DefaultPromise[T]]) {
        val dp = other.asInstanceOf[DefaultPromise[T]]
        val resolved = dp.value0
        if (resolved ne null) tryComplete0(get(), resolved)
        else dp.dispatchOrAddCallbacks(dp.get(), new Transformation[T, Unit](Xform_completeWith, this, InternalCallbackExecutor))
      } else other.onComplete(this)(InternalCallbackExecutor)

      this
    }

    /** Tries to add the callback, if already completed, it dispatches the callback to be executed.
     *  Used by `onComplete()` to add callbacks to a promise.
     */
    @tailrec private final def dispatchOrAddCallbacks(state: AnyRef, callback: Transformation[T, _]): callback.type =
      if (state.isInstanceOf[Try[T]]) {
        submitWithValue(callback, state.asInstanceOf[Try[T]]) // invariant: callbacks should never be Noop here
        callback
      } else /* if (state.isInstanceOf[Callbacks[T]]) */ {
        if(compareAndSet(state, if (state ne Noop) concatCallbacks(callback, state.asInstanceOf[Callbacks[T]]) else callback)) callback
        else dispatchOrAddCallbacks(get(), callback)
      }

    // IMPORTANT: Noop should never be passed in here, neither as callback OR as rest
    private[this] final def concatCallbacks(callback: Transformation[T, _], rest: Callbacks[T]): Callbacks[T] =
      new ManyCallbacks[T](callback, rest)

    // IMPORTANT: Noop should not be passed in here, `callbacks` cannot be null
    @tailrec
    private[this] final def submitWithValue(callbacks: Callbacks[T], resolved: Try[T]): Unit =
      if(callbacks.isInstanceOf[ManyCallbacks[T]]) {
        val m: ManyCallbacks[T] = callbacks.asInstanceOf[ManyCallbacks[T]]
        m.first.submitWithValue(resolved)
        submitWithValue(m.rest, resolved)
      } else {
        callbacks.asInstanceOf[Transformation[T, _]].submitWithValue(resolved)
      }
  }

  // Constant byte tags for unpacking transformation function inputs or outputs
  // These need to be Ints to get compiled into constants, but we don't want to
  // pay 32-bit to store them so we convert to/from Byte
  final val Xform_noop          = 0
  final val Xform_map           = 1
  final val Xform_flatMap       = 2
  final val Xform_transform     = 3
  final val Xform_transformWith = 4
  final val Xform_foreach       = 5
  final val Xform_onComplete    = 6
  final val Xform_recover       = 7
  final val Xform_recoverWith   = 8
  final val Xform_filter        = 9
  final val Xform_collect       = 10
  final val Xform_completeWith  = 11

    /* Marker trait
   */
  sealed trait Callbacks[-T]

  final class ManyCallbacks[-T](final val first: Transformation[T,_], final val rest: Callbacks[T]) extends Callbacks[T] {
    override final def toString: String = "ManyCallbacks"
  }

  private[this] final val Noop = new Transformation[Nothing, Nothing](Xform_noop, null, InternalCallbackExecutor)

  /**
   * A Transformation[F, T] receives an F (it is a Callback[F]) and applies a transformation function to that F,
   * Producing a value of type T (it is a Promise[T]).
   * In order to conserve allocations, indirections, and avoid introducing bi/mega-morphicity the transformation
   * function's type parameters are erased, and the _xform tag will be used to reify them.
   **/
  final class Transformation[-F, T] private[this] (
    private[this] final var _fun: Any => Any,
    private[this] final var _ec: ExecutionContext,
    private[this] final var _arg: Try[F],
    private[this] final val _xform: Byte
  ) extends DefaultPromise[T]() with Callbacks[F] with Runnable with Batchable with OnCompleteRunnable {
    final def this(xform: Int, f: _ => _, ec: ExecutionContext) = this(f.asInstanceOf[Any => Any], ec.prepare(), null, xform.toByte)

    private final def xform: Int = _xform.toInt
    private final def fun: Any => Any = _fun

    final def benefitsFromBatching: Boolean = _xform != Xform_onComplete && _xform != Xform_foreach

    private final def submitWithValueToEC(resolved: Try[F]): Unit = {
      _arg = resolved
      val e = _ec
      try e.execute(this) /* Safe publication of _arg, _fun, _ec */
      catch {
        case t: Throwable => handleFailure(t, e)
      }
    }

    @tailrec private[this] final def submitWithValue0(transformation: Transformation[F,_], resolved: Try[F]): Unit =
      if (transformation.xform != Xform_completeWith) transformation.submitWithValueToEC(resolved)
      else { // Traverse the completion chain and complete as we walk the chain
        val tstate = transformation.get()
        if (tstate.isInstanceOf[Transformation[F,_]] && (tstate ne Noop)) {
          val candidate_next = tstate.asInstanceOf[Transformation[F,_]].fun.asInstanceOf[Transformation[F,_]]
          val next = if (transformation.compareAndSet(tstate, resolved)) candidate_next else transformation
          submitWithValue0(next, resolved)
        } else
          transformation.submitWithValueToEC(resolved)
      }

    // Gets invoked when a value is available, schedules it to be run():ed by the ExecutionContext
    // submitWithValue *happens-before* run(), through ExecutionContext.execute.
    // Invariant: _arg is `ExecutionContext`, and non-null. `this` ne Noop.
    // requireNonNull(resolved) will hold as guarded by `resolve`
    final def submitWithValue(resolved: Try[F]): this.type = {
      submitWithValue0(this, resolved)
      this
    }

    private[this] final def handleFailure(t: Throwable, e: ExecutionContext): Unit = {
      _fun = null // allow to GC
      _arg = null // see above
      _ec  = null // see above again
      val wasInterrupted = t.isInstanceOf[InterruptedException]
      if (wasInterrupted || NonFatal(t)) {
        val completed = tryComplete0(get(), resolve(Failure(t)))
        if (completed && wasInterrupted) Thread.currentThread.interrupt()

        // Report or rethrow failures which are unlikely to otherwise be noticed
        if (_xform == Xform_foreach || _xform == Xform_onComplete || !completed)
          e.reportFailure(t)
      } else throw t
    }

    @inline private[this] final def completeWithLink(f: Future[T]): Try[T] = {
      completeWith(f)
      null
    }

    // Gets invoked by the ExecutionContext, when we have a value to transform.
    override final def run(): Unit = {
      val v   = _arg
      val fun = _fun
      val ec  = _ec
      try {
        val resolvedResult: Try[T] =
          (_xform.toInt: @switch) match {
            case Xform_noop          =>
              null
            case Xform_map           =>
              if (v.isInstanceOf[Success[F]]) Success(fun(v.asInstanceOf[Success[F]].value).asInstanceOf[T]) else v.asInstanceOf[Failure[T]]
            case Xform_flatMap       =>
              if (v.isInstanceOf[Success[F]]) completeWithLink(fun(v.asInstanceOf[Success[F]].value).asInstanceOf[Future[T]])
              else v.asInstanceOf[Failure[T]] // Already resolved
            case Xform_transform     =>
              resolve(fun(v).asInstanceOf[Try[T]])
            case Xform_transformWith =>
              completeWithLink(fun(v).asInstanceOf[Future[T]])
            case Xform_foreach       =>
              if (v.isInstanceOf[Success[F]]) fun(v.asInstanceOf[Success[F]].value)
              null
            case Xform_onComplete    =>
              fun(v)
              null
            case Xform_recover       =>
              resolve(v.recover(fun.asInstanceOf[PartialFunction[Throwable, F]]).asInstanceOf[Try[T]]) //recover F=:=T
            case Xform_recoverWith   =>
              if (v.isInstanceOf[Failure[F]]) {
                val f = fun.asInstanceOf[PartialFunction[Throwable, Future[T]]].applyOrElse(v.asInstanceOf[Failure[F]].exception, Future.recoverWithFailed)
                if (f ne Future.recoverWithFailedMarker) completeWithLink(f)
                else v.asInstanceOf[Failure[T]]
              } else v.asInstanceOf[Success[T]]
            case Xform_filter        =>
              if (v.isInstanceOf[Failure[F]] || fun.asInstanceOf[F => Boolean](v.asInstanceOf[Success[F]].value)) v.asInstanceOf[Try[T]]
              else Future.filterFailure // Safe for unresolved completes
            case Xform_collect       =>
              if (v.isInstanceOf[Success[F]]) Success(fun.asInstanceOf[PartialFunction[F, T]].applyOrElse(v.asInstanceOf[Success[F]].value, Future.collectFailed))
              else v.asInstanceOf[Failure[T]] // Already resolved
            case Xform_completeWith  =>
              fun(v)
              null
            case _                   =>
              Failure(new IllegalStateException("BUG: encountered transformation promise with illegal type: " + _xform))
          }
        if (resolvedResult ne null)
          tryComplete0(get(), resolvedResult)
        _fun = null // allow to GC
        _arg = null // see above
        _ec  = null // see above
      } catch {
        case t: Throwable => handleFailure(t, ec)
      }
    }
  }
}
