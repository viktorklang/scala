/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2003-2013, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */

package scala.concurrent.impl

import scala.concurrent.{ Future, ExecutionContext, CanAwait, OnCompleteRunnable, TimeoutException, ExecutionException }
import scala.concurrent.Future.InternalCallbackExecutor
import scala.concurrent.duration.{ Duration, FiniteDuration }
import scala.annotation.{ tailrec, switch, unchecked }
import scala.annotation.unchecked.uncheckedVariance
import scala.util.control.{ NonFatal, ControlThrowable }
import scala.util.{ Try, Success, Failure }
import scala.runtime.NonLocalReturnControl
import scala.language.higherKinds
import java.util.concurrent.locks.AbstractQueuedSynchronizer
import java.util.concurrent.atomic.AtomicReference
import java.util.Objects.requireNonNull

private[concurrent] final object Promise {
   /**
    * Latch used to implement waiting on a DefaultPromise's result.
    *
    * Inspired by: http://gee.cs.oswego.edu/cgi-bin/viewcvs.cgi/jsr166/src/main/java/util/concurrent/locks/AbstractQueuedSynchronizer.java
    * Written by Doug Lea with assistance from members of JCP JSR-166
    * Expert Group and released to the public domain, as explained at
    * http://creativecommons.org/publicdomain/zero/1.0/
    */
    private final class CompletionLatch[T] extends AbstractQueuedSynchronizer with (Try[T] => Unit) {
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

    private final class Link[T](to: DefaultPromise[T]) extends AtomicReference[DefaultPromise[T]](to) {

      final def promise(owner: DefaultPromise[T]): DefaultPromise[T] = compressedRoot(this, owner)

      @tailrec private[this] def root(of: DefaultPromise[T], owner: DefaultPromise[T]): DefaultPromise[T] = {
        val value = of.get()
        if (value.isInstanceOf[Link[T]]) root(value.asInstanceOf[Link[T]].get(), owner)
        else if (value.isInstanceOf[Try[T]]) {
          unlink(owner, value.asInstanceOf[Try[T]]) // FIXME: Verify that this won't cause any unforeseen problems
          owner // we are now unlinked
        } else of
      }

      @tailrec private[this] final def compressedRoot(linked: Link[T], owner: DefaultPromise[T]): DefaultPromise[T] = {
        val current = linked.get()
        val target = root(current, owner)
        // This tries to make the ideal Links point directly to the *end* of the link chain. Can we do better when the end is resolved?
        if ((current eq target) || (target eq owner) || compareAndSet(current, target)) target
        else compressedRoot(linked, owner)
      }

      @tailrec final def link(target: DefaultPromise[T], owner: DefaultPromise[T]): DefaultPromise[T] = {
        val current = get()
        val newTarget = root(target, owner)
        if ((current eq newTarget) || compareAndSet(current, newTarget)) newTarget
        else link(target, owner)
      }

      @tailrec private[this] final def unlink(owner: DefaultPromise[T], value: Try[T]): Boolean = {
        val l = owner.get()
        if (l.isInstanceOf[Link[T]]) {
          if (owner.compareAndSet(l, value))
            unlink(l.asInstanceOf[Link[T]].get(), value)
          else
            unlink(owner, value)
        } else true
      }
    }

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
  class DefaultPromise[T] private[this] (initial: AnyRef) extends AtomicReference[AnyRef](initial) with scala.concurrent.Promise[T] with scala.concurrent.Future[T] {
    /**
     * Constructs a new, completed, Promise.
     */
    def this(result: Try[T]) = this(resolve(result): AnyRef)

    /**
     * Constructs a new, un-completed, Promise.
     */
    def this() = this(Noop: AnyRef)

    /**
     * Returns the associaed `Future` with this `Promise`
     */
    override def future: Future[T] = this

    override def transform[S](f: Try[T] => Try[S])(implicit executor: ExecutionContext): Future[S] =
      dispatchOrAddCallbacks(new XformPromise[T, S](Xform_transform, f, executor))

    override def transformWith[S](f: Try[T] => Future[S])(implicit executor: ExecutionContext): Future[S] =
      dispatchOrAddCallbacks(new XformPromise[T, S](Xform_transformWith, f, executor))

    override def onFailure[U](@deprecatedName('callback) pf: PartialFunction[Throwable, U])(implicit executor: ExecutionContext): Unit =
      if (!get.isInstanceOf[Success[T]]) super[Future].onFailure(pf) // Short-circuit if we get a Failure

    override def onSuccess[U](pf: PartialFunction[T, U])(implicit executor: ExecutionContext): Unit = 
      if (!get.isInstanceOf[Failure[T]]) super[Future].onSuccess(pf)  // Short-circuit if we get a Success

    override def foreach[U](f: T => U)(implicit executor: ExecutionContext): Unit =
      if (!get.isInstanceOf[Failure[T]]) dispatchOrAddCallbacks(new XformPromise[T, Unit](Xform_foreach, f, executor))

    override def flatMap[S](f: T => Future[S])(implicit executor: ExecutionContext): Future[S] = 
      if (!get.isInstanceOf[Failure[T]]) dispatchOrAddCallbacks(new XformPromise[T, S](Xform_flatMap, f, executor))
      else this.asInstanceOf[Future[S]]

    override def map[S](f: T => S)(implicit executor: ExecutionContext): Future[S] =
      if (!get.isInstanceOf[Failure[T]]) dispatchOrAddCallbacks(new XformPromise[T, S](Xform_map, f, executor))
      else this.asInstanceOf[Future[S]]

    override def filter(@deprecatedName('pred) p: T => Boolean)(implicit executor: ExecutionContext): Future[T] =
      if (!get.isInstanceOf[Failure[T]]) super[Future].filter(p) // Short-circuit if we get a Success
      else this

    override def collect[S](pf: PartialFunction[T, S])(implicit executor: ExecutionContext): Future[S] =
      if (!get.isInstanceOf[Failure[T]]) super[Future].collect(pf) // Short-circuit if we get a Success
      else this.asInstanceOf[Future[S]]

    override def recoverWith[U >: T](pf: PartialFunction[Throwable, Future[U]])(implicit executor: ExecutionContext): Future[U] =
      if (!get.isInstanceOf[Success[T]]) dispatchOrAddCallbacks(new XformPromise[T, U](Xform_recoverWith, pf, executor)) // Short-circuit if we get a Failure
      else this.asInstanceOf[Future[U]]

    override def recover[U >: T](pf: PartialFunction[Throwable, U])(implicit executor: ExecutionContext): Future[U] =
      if (!get.isInstanceOf[Success[T]]) dispatchOrAddCallbacks(new XformPromise[T, U](Xform_recover, pf, executor)) // Short-circuit if we get a Failure
      else this.asInstanceOf[Future[U]]

    /* TODO: is this worth overriding?
    override def mapTo[S](implicit tag: scala.reflect.ClassTag[S]): Future[S] =
      if (!get.isInstanceOf[Failure[T]]) super[Future].mapTo[S](tag) // Short-circuit if we get a Success
      else this.asInstanceOf[Future[S]]*/


    override def toString: String = toString0

    @tailrec private final def toString0: String = {
      val state = get()
      if (state.isInstanceOf[Try[T]]) "Future("+state+")"
      else if (state.isInstanceOf[Link[T]]) state.asInstanceOf[Link[T]].promise(this).toString0
      else /*if (state.isInstanceOf[Callbacks[T]]) */ "Future(<not completed>)"
    }

    /** Try waiting for this promise to be completed.
     * Returns true if it completed at the end of this waiting time.
     * Does not allow Duration.Undefined as a parameter, and throws IllegalArgumentException
     * if it is passed in.
     */
    protected final def tryAwait(atMost: Duration): Boolean = tryAwait0(atMost) ne null

    private[this] final def tryAwait0(atMost: Duration): Try[T] =
      if (atMost ne Duration.Undefined) {
        val v = value0
        if ((v ne null) || atMost <= Duration.Zero) v
        else {
          val l = new CompletionLatch[T]()
          onComplete(l)(InternalCallbackExecutor)

          if (atMost.isFinite)
            l.tryAcquireSharedNanos(1, atMost.toNanos)
          else
            l.acquireSharedInterruptibly(1)

          l.result
        }
      } else throw new IllegalArgumentException("Cannot wait for Undefined duration of time")

    @throws(classOf[TimeoutException])
    @throws(classOf[InterruptedException])
    final def ready(atMost: Duration)(implicit permit: CanAwait): this.type = {
      val v = tryAwait0(atMost)
      if (v ne null) this
      else throw new TimeoutException("Future timed out after [" + atMost + "]")
    }

    @throws(classOf[Exception])
    final def result(atMost: Duration)(implicit permit: CanAwait): T = {
      val v = tryAwait0(atMost)
      if (v ne null) v.get // returns the value, or throws the contained exception
      else throw new TimeoutException("Future timed out after [" + atMost + "]")
    }

    override final def isCompleted: Boolean = value0 ne null

    override def value: Option[Try[T]] = Option(value0)

    @tailrec
    private final def value0: Try[T] = {
      val state = get()
      if (state.isInstanceOf[Try[T]]) state.asInstanceOf[Try[T]]
      else if (state.isInstanceOf[Link[T]]) state.asInstanceOf[Link[T]].promise(this).value0
      else /*if (state.isInstanceOf[Callbacks[T]])*/ null
    }

    override final def tryComplete(value: Try[T]): Boolean = {
      val state = get()
      if (state.isInstanceOf[Try[T]]) false
      else /*if (state.isInstanceOf[Link[T]]*/
        tryComplete0(state, resolve(value))
    }

    @tailrec
    private final def tryComplete0(state: AnyRef, resolved: Try[T]): Boolean = {
      if (state.isInstanceOf[Callbacks[T]]) {
        if (compareAndSet(state, resolved)) {
          if (state ne Noop) submitWithValue(state.asInstanceOf[Callbacks[T]], resolved) // TODO: if-check needed performance-wise?
          true
        } else tryComplete0(get(), resolved)
      } else if (state.isInstanceOf[Link[T]]) {
        val p = state.asInstanceOf[Link[T]].promise(this)
        p.tryComplete0(p.get(), resolved) // Use this to get tailcall optimization and avoid re-resolution
      } else /* if(state.isInstanceOf[Try[T]]) */ false
    }

    override final def onComplete[U](func: Try[T] => U)(implicit executor: ExecutionContext): Unit =
      dispatchOrAddCallbacks(new XformPromise[T, Unit](Xform_onComplete, func, executor))

    /** Tries to add the callback, if already completed, it dispatches the callback to be executed.
     *  Used by `onComplete()` to add callbacks to a promise and by `link()` to transfer callbacks
     *  to the root promise when linking two promises together.
     */
    @tailrec private final def dispatchOrAddCallbacks[C <: Callbacks[T]](callbacks: C): C = {
      val state = get()
      if (state.isInstanceOf[Try[T]]) {
        submitWithValue(callbacks, state.asInstanceOf[Try[T]])
        callbacks
      } else if (state.isInstanceOf[Callbacks[T]]) {
        val newCallbacks = if (state eq Noop) callbacks else new ManyCallbacks[T](callbacks, state.asInstanceOf[Callbacks[T]])
        if(compareAndSet(state, newCallbacks)) callbacks
        else dispatchOrAddCallbacks(callbacks)
      } else /*if (state.isInstanceOf[Link[T]])*/
        state.asInstanceOf[Link[T]].promise(this).dispatchOrAddCallbacks(callbacks)
    }

    private[this] final def submitWithValue(cb: Callbacks[T], v: Try[T]): Unit = 
      if (cb.isInstanceOf[XformPromise[T,_]]) cb.asInstanceOf[XformPromise[T,_]].submitWithValue(v)
      else /*if (cb.isInstanceOf[ManyCallbacks[T]])*/ {
        val m = cb.asInstanceOf[ManyCallbacks[T]]
        submitWithValue(m.first, v) // TODO: this will grow the stack—needs real-world proofing
        submitWithValue(m.last, v)
      }

    /** Link this promise to the root of another promise.
     */
    protected[concurrent] final def linkRootOf(target: DefaultPromise[T]): Unit =
      if (this ne target) {
        val state = get()
        if (state.isInstanceOf[Try[T]]) {
          if (!target.tryComplete(state.asInstanceOf[Try[T]]))
            throw new IllegalStateException("Cannot link completed promises together")
        } else if (state.isInstanceOf[Link[T]]) state.asInstanceOf[Link[T]].link(target, this)
        else /*if (state.isInstanceOf[Callbacks[T]]) */
          tryLink(state, new Link(target))
      }

    /** Link this promise to another promise so that both promises share the same
     *  externally-visible state. Depending on the current state of this promise, this
     *  may involve different things. For example, any onComplete listeners will need
     *  to be transferred.
     *
     *  If this promise is already completed, then the same effect as linking -
     *  sharing the same completed value - is achieved by simply sending this
     *  promise's result to the target promise.
     *
     *  Should only be called by linkRootOf().
     */
    @tailrec
    private[this] final def tryLink(state: AnyRef, target: Link[T]): Unit = {
      val promise = target.promise(this)
      if (this ne promise) {
        if (state.isInstanceOf[Callbacks[T]]) {
          if (compareAndSet(state, target)) {
            if (state ne Noop) // TODO: needed?
              promise.dispatchOrAddCallbacks(state.asInstanceOf[Callbacks[T]])
          } else tryLink(get(), target)
        } else linkRootOf(promise) // If the current state is not Callbacks, fall back to normal linking
      }
    }
  }

  // Byte tags for unpacking transformation function inputs or outputs
  final val Xform_map           = 0
  final val Xform_flatMap       = 1
  final val Xform_transform     = 2
  final val Xform_transformWith = 3
  final val Xform_foreach       = 4
  final val Xform_onComplete    = 5
  final val Xform_recover       = 6
  final val Xform_recoverWith   = 7
  final val Xform_filter        = 8
  final val Xform_collect       = 9

    /* Marker trait
   */
  sealed trait Callbacks[-T]

  private[this] final val Noop = new XformPromise[Nothing, Nothing](Int.MinValue, null, InternalCallbackExecutor)

  final class XformPromise[-F, T] private[this] (
    private[this] final var _fun: Any => Any,
    private[this] final var _arg: AnyRef,
    private[this] final val _xform: Byte
  ) extends DefaultPromise[T]() with Callbacks[F] with Runnable with OnCompleteRunnable {
    def this(xform: Int, f: _ => _, ec: ExecutionContext) = this(f.asInstanceOf[Any => Any], ec.prepare(): AnyRef, xform.asInstanceOf[Byte])

    // Gets invoked when a value is available, schedules it to be run():ed by the ExecutionContext
    // submitWithValue *happens-before* run(), through ExecutionContext.execute.
    // Invariant: _arg is `ExecutionContext`
    // requireNonNull(v) will hold as guarded by `resolve`
    final def submitWithValue(resolved: Try[F]): Unit =
      if (_fun ne null) {
        val executor = _arg.asInstanceOf[ExecutionContext]
        try {
          _arg = resolved
          executor.execute(this) // Safe publication of _arg = v (and _fun)
        } catch {
          case t if NonFatal(t) =>
            if (!tryFailure(t))
              executor.reportFailure(t)
        }
      }

    // Gets invoked by the ExecutionContext, when we have a value to transform.
    // Invariant: if (_arg.isInstanceOf[Try[F]] && (_fun ne null))
    override final def run(): Unit =
        try {
          val v = _arg.asInstanceOf[Try[F]]
          (_xform.asInstanceOf[Int]: @switch) match {
            case Xform_map           => doMap(v)
            case Xform_flatMap       => doFlatMap(v)
            case Xform_transform     => doTransform(v)
            case Xform_transformWith => doTransformWith(v)
            case Xform_foreach       => doForeach(v)
            case Xform_onComplete    => doOnComplete(v)
            case Xform_recover       => doRecover(v)
            case Xform_recoverWith   => doRecoverWith(v)
            case Xform_filter        => doFilter(v)
            case Xform_collect       => doCollect(v)
            case _                   => doAbort(v)
          }
        } catch {
          case t if NonFatal(t) => tryFailure(t)
        } finally { // allow these to GC
          _fun = null
          _arg = null
        }

    override final def toString: String = super[DefaultPromise].toString

    private[this] final def completeFuture(f: Future[T]): Unit =
      if(f.isInstanceOf[DefaultPromise[T]]) f.asInstanceOf[DefaultPromise[T]].linkRootOf(this)
      else completeWith(f)

    private[this] final def doMap(v: Try[F]): Unit = tryComplete(v.map(_fun.asInstanceOf[F => T]))

    private[this] final def doFlatMap(v: Try[F]): Unit =
      if (v.isInstanceOf[Success[F]]) completeFuture(_fun(v.asInstanceOf[Success[F]].value).asInstanceOf[Future[T]])
      else tryComplete(v.asInstanceOf[Try[T]])

    private[this] final def doTransform(v: Try[F]): Unit = tryComplete(_fun(v).asInstanceOf[Try[T]])

    private[this] final def doTransformWith(v: Try[F]): Unit = completeFuture(_fun(v).asInstanceOf[Future[T]])

    private[this] final def doForeach(v: Try[F]): Unit = {
      v foreach _fun
      tryComplete(Future.successOfUnit.asInstanceOf[Try[T]]) //FIXME do this need to ever get completed?
    }

    private[this] final def doOnComplete(v: Try[F]): Unit = {
      _fun(v)
      tryComplete(Future.successOfUnit.asInstanceOf[Try[T]]) //FIXME do this need to ever get completed?
    }

    private[this] final def doRecover(v: Try[F]): Unit =
      tryComplete(v.recover(_fun.asInstanceOf[PartialFunction[Throwable, F]]).asInstanceOf[Try[T]]) //recover F=:=T

    private[this] final def doRecoverWith(v: Try[F]): Unit = //recoverWith F=:=T
      if (v.isInstanceOf[Failure[F]]) {
        val fail = v.asInstanceOf[Failure[F]]
        val r = _fun.asInstanceOf[PartialFunction[Throwable, Future[T]]].applyOrElse(fail.exception, Future.recoverWithFailed)
        if (r ne Future.recoverWithFailedMarker) completeFuture(r)
        else tryComplete(fail.asInstanceOf[Failure[T]])
      } else tryComplete(v.asInstanceOf[Try[T]])

    private[this] final def doFilter(v: Try[F]): Unit =
      tryComplete(
        if (v.isInstanceOf[Failure[F]] || _fun.asInstanceOf[F => Boolean](v.asInstanceOf[Success[F]].value)) v.asInstanceOf[Try[T]]
        else Future.filterFailure
      )

    private[this] final def doCollect(v: Try[F]): Unit =
      tryComplete(
        if (v.isInstanceOf[Success[F]]) Success(_fun.asInstanceOf[PartialFunction[F, T]].applyOrElse(v.asInstanceOf[Success[F]].value, Future.collectFailed))
        else v.asInstanceOf[Try[T]]
      )

    private[this] final def doAbort(v: Try[F]): Unit =
      tryComplete(Failure(new IllegalStateException("BUG: encountered transformation promise with illegal type: " + _xform)))
  }

  final class ManyCallbacks[-T](final val first: Callbacks[T], final val last: Callbacks[T]) extends Callbacks[T] {
    override final def toString: String = "ManyCallbacks"
  }
}
