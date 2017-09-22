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
import scala.util.control.NonFatal
import scala.util.{ Try, Success, Failure }

import java.util.concurrent.locks.AbstractQueuedSynchronizer
import java.util.concurrent.atomic.AtomicReference

private[concurrent] final object Promise {
  private[this] final def executionFailure[T](msg: String, cause: Throwable): Failure[T] =
    Failure(new ExecutionException(msg, cause))

  private[this] final def resolveFailure[T](f: Failure[T]): Try[T] =
    f.exception match {
      case t: scala.runtime.NonLocalReturnControl[T @unchecked] => Success(t.value)
      case t: scala.util.control.ControlThrowable    => executionFailure("Boxed ControlThrowable", t)
      case t: InterruptedException                   => executionFailure("Boxed InterruptedException", t)
      case e: Error                                  => executionFailure("Boxed Error", e)
      case _                                         => f
    }

  private final def resolveTry[T](source: Try[T]): Try[T] = 
    source match {
      case null                     => throw new IllegalArgumentException("Cannot complete a Promise with `null`")
      case f: Failure[T @unchecked] => resolveFailure(f)
      case _                        => source
    }

  final def transformWithDefaultPromise[T, S](f: Try[T] => Future[S]): DefaultPromise[S] with (Try[T] => Unit) =
   new DefaultPromise[S] with (Try[T] => Unit) {
      private[this] final var fun = f
      override final def apply(v: Try[T]): Unit = if (fun ne null) {
        try fun(v) match {
          case dp: DefaultPromise[S @unchecked] => dp.linkRootOf(this) // If possible, link DefaultPromises to avoid space leaks
          case fut => this completeWith fut
        } catch { case NonFatal(t) => this failure t } finally { fun = null }
      }
      override final def toString: String = super[DefaultPromise].toString
    }

  final def transformImpl[T, S](future: Future[T], f: Try[T] => Try[S])(implicit ec: ExecutionContext): Future[S] = {
    val p = transformDefaultPromise(f)
    future.onComplete(p)
    p.future
  }

  final def transformDefaultPromise[T, S](f: Try[T] => Try[S]): DefaultPromise[S] with (Try[T] => Unit) =
    new DefaultPromise[S] with (Try[T] => Unit) {
      private[this] var fun = f
      override final def apply(result: Try[T]): Unit = 
        if (fun ne null) this.complete(try fun(result) catch { case NonFatal(t) => Failure(t) } finally { fun = null })
      override final def toString: String = super[DefaultPromise].toString
    }

   /**
    * Latch used to implement waiting on a DefaultPromise's result.
    *
    * Inspired by: http://gee.cs.oswego.edu/cgi-bin/viewcvs.cgi/jsr166/src/main/java/util/concurrent/locks/AbstractQueuedSynchronizer.java
    * Written by Doug Lea with assistance from members of JCP JSR-166
    * Expert Group and released to the public domain, as explained at
    * http://creativecommons.org/publicdomain/zero/1.0/
    */
    private final class CompletionLatch[T] extends AbstractQueuedSynchronizer with (Try[T] => Unit) {
      override protected def tryAcquireShared(ignored: Int): Int = if (getState != 0) 1 else -1
      override protected def tryReleaseShared(ignore: Int): Boolean = {
        setState(1)
        true
      }
      override def apply(ignored: Try[T]): Unit = releaseShared(1)
    }


  /** Default promise implementation.
   *
   *  A DefaultPromise has three possible states. It can be:
   *
   *  1. Incomplete, with an associated list of callbacks waiting on completion.
   *  2. Complete, with a result.
   *  3. Linked to another DefaultPromise.
   *
   *  If a DefaultPromise is linked to another DefaultPromise, it will
   *  delegate all its operations to that other promise. This means that two
   *  DefaultPromises that are linked will appear, to external callers, to have
   *  exactly the same state and behaviour. For instance, both will appear as
   *  incomplete, or as complete with the same result value.
   *
   *  A DefaultPromise stores its state entirely in the AnyRef cell exposed by
   *  AtomicReference. The type of object stored in the cell fully describes the
   *  current state of the promise.
   *
   *  1. Callbacks - The promise is incomplete and has zero or more callbacks
   *     to call when it is eventually completed.
   *  2. Try[T] - The promise is complete and now contains its value.
   *  3. DefaultPromise[T] - The promise is linked to another promise.
   *
   * The ability to link DefaultPromises is needed to prevent memory leaks when
   * using Future.flatMap. The previous implementation of Future.flatMap used
   * onComplete handlers to propagate the ultimate value of a flatMap operation
   * to its promise. Recursive calls to flatMap built a chain of onComplete
   * handlers and promises. Unfortunately none of the handlers or promises in
   * the chain could be collected until the handlers had been called and
   * detached, which only happened when the final flatMap future was completed.
   * (In some situations, such as infinite streams, this would never actually
   * happen.) Because of the fact that the promise implementation internally
   * created references between promises, and these references were invisible to
   * user code, it was easy for user code to accidentally build large chains of
   * promises and thereby leak memory.
   *
   * The problem of leaks is solved by automatically breaking these chains of
   * promises, so that promises don't refer to each other in a long chain. This
   * allows each promise to be individually collected. The idea is to "flatten"
   * the chain of promises, so that instead of each promise pointing to its
   * neighbour, they instead point directly the promise at the root of the
   * chain. This means that only the root promise is referenced, and all the
   * other promises are available for garbage collection as soon as they're no
   * longer referenced by user code.
   *
   * To make the chains flattenable, the concept of linking promises together
   * needed to become an explicit feature of the DefaultPromise implementation,
   * so that the implementation to navigate and rewire links as needed. The idea
   * of linking promises is based on the [[Twitter promise implementation
   * https://github.com/twitter/util/blob/master/util-core/src/main/scala/com/twitter/util/Promise.scala]].
   *
   * In practice, flattening the chain cannot always be done perfectly. When a
   * promise is added to the end of the chain, it scans the chain and links
   * directly to the root promise. This prevents the chain from growing forwards
   * But the root promise for a chain can change, causing the chain to grow
   * backwards, and leaving all previously-linked promise pointing at a promise
   * which is no longer the root promise.
   *
   * To mitigate the problem of the root promise changing, whenever a promise's
   * methods are called, and it needs a reference to its root promise it calls
   * the `compressedRoot()` method. This method re-scans the promise chain to
   * get the root promise, and also compresses its links so that it links
   * directly to whatever the current root promise is. This ensures that the
   * chain is flattened whenever `compressedRoot()` is called. And since
   * `compressedRoot()` is called at every possible opportunity (when getting a
   * promise's value, when adding an onComplete handler, etc), this will happen
   * frequently. Unfortunately, even this eager relinking doesn't absolutely
   * guarantee that the chain will be flattened and that leaks cannot occur.
   * However eager relinking does greatly reduce the chance that leaks will
   * occur.
   *
   * Future.flatMap links DefaultPromises together by calling the `linkRootOf`
   * method. This is the only externally visible interface to linked
   * DefaultPromises, and `linkedRootOf` is currently only designed to be called
   * by Future.flatMap.
   */
  // Left non-final to enable addition of extra fields by Java/Scala converters
  // in scala-java8-compat.
  class DefaultPromise[T] private[this] (init: AnyRef) extends AtomicReference[AnyRef](init) with scala.concurrent.Promise[T] with scala.concurrent.Future[T] {

    /**
     * Constructs a new, uncompleted, Promise.
     */
    def this() = this(NoopCallback: AnyRef)

    /**
     * Constructs a new, completed, Promise.
     */
    def this(result: Try[T]) = this(resolveTry(result): AnyRef)

    /**
     * Returns the associaed `Future` with this `Promise`
     */
    override def future: Future[T] = this

    override def transform[S](f: Try[T] => Try[S])(implicit executor: ExecutionContext): Future[S] = {
      val p = Promise.transformDefaultPromise(f)
      onComplete(p)
      p.future
    }

    override def transformWith[S](f: Try[T] => Future[S])(implicit executor: ExecutionContext): Future[S] = {
      val p = transformWithDefaultPromise(f)
      onComplete(p)
      p.future
    }

    override def toString: String = toString0

    @tailrec private final def toString0: String = get() match {
      case c: Try[T @unchecked] => s"Future($c)"
      case dp: DefaultPromise[T @unchecked] => compressedRoot(dp).toString0
      case _ => "Future(<not completed>)"
    }

    /** Get the root promise for this promise, compressing the link chain to that
     *  promise if necessary.
     *
     *  For promises that are not linked, the result of calling
     *  `compressedRoot()` will the promise itself. However for linked promises,
     *  this method will traverse each link until it locates the root promise at
     *  the base of the link chain.
     *
     *  As a side effect of calling this method, the link from this promise back
     *  to the root promise will be updated ("compressed") to point directly to
     *  the root promise. This allows intermediate promises in the link chain to
     *  be garbage collected. Also, subsequent calls to this method should be
     *  faster as the link chain will be shorter.
     */
    private def compressedRoot(): DefaultPromise[T] =
      get() match {
        case linked: DefaultPromise[T @unchecked] => compressedRoot(linked)
        case _ => this
      }

    @tailrec
    private[this] final def compressedRoot(linked: DefaultPromise[T]): DefaultPromise[T] = {
      val target = linked.root
      if (linked eq target) target
      else if (compareAndSet(linked, target)) target
      else {
        get() match {
          case newLinked: DefaultPromise[T @unchecked] => compressedRoot(newLinked)
          case _ => this
        }
      }
    }

    /** Get the promise at the root of the chain of linked promises. Used by `compressedRoot()`.
     *  The `compressedRoot()` method should be called instead of this method, as it is important
     *  to compress the link chain whenever possible.
     */
    @tailrec
    private def root: DefaultPromise[T] =
      get() match {
        case linked: DefaultPromise[T @unchecked] => linked.root
        case _ => this
      }

    /** Try waiting for this promise to be completed.
     */
    protected final def tryAwait(atMost: Duration): Boolean = if (!isCompleted) {
      import Duration.Undefined
      atMost match {
        case e if e eq Undefined => throw new IllegalArgumentException("cannot wait for Undefined period")
        case Duration.Inf        =>
          val l = new CompletionLatch[T]()
          onComplete(l)(InternalCallbackExecutor)
          l.acquireSharedInterruptibly(1)
        case Duration.MinusInf   => // Drop out
        case f: FiniteDuration   =>
          if (f > Duration.Zero) {
            val l = new CompletionLatch[T]()
            onComplete(l)(InternalCallbackExecutor)
            l.tryAcquireSharedNanos(1, f.toNanos)
          }
      }

      isCompleted
    } else true // Already completed

    @throws(classOf[TimeoutException])
    @throws(classOf[InterruptedException])
    final def ready(atMost: Duration)(implicit permit: CanAwait): this.type =
      if (tryAwait(atMost)) this
      else throw new TimeoutException("Future timed out after [" + atMost + "]")

    @throws(classOf[Exception])
    final def result(atMost: Duration)(implicit permit: CanAwait): T = {
      ready(atMost)
      value0.get // ready throws TimeoutException if timeout so value0.get is safe here
    }

    override final def isCompleted: Boolean = value0 ne null

    override def value: Option[Try[T]] = Option(value0)

    @tailrec
    private final def value0: Try[T] = get() match {
      case c: Try[T @unchecked]             => c
      case dp: DefaultPromise[T @unchecked] => compressedRoot(dp).value0
      case cbs: Callbacks[T @unchecked]     => null
    }

    override final def tryComplete(value: Try[T]): Boolean =
      tryComplete0(resolveTry(value))

    @tailrec
    private final def tryComplete0(v: Try[T]): Boolean =
      get() match {
        case cbs: Callbacks[T @unchecked]      =>
          if (compareAndSet(cbs, v)) {
            cbs.submitWithValue(v)
            true
          } else tryComplete0(v)
        case _: Try[T @unchecked]             => false
        case dp: DefaultPromise[T @unchecked] => compressedRoot(dp).tryComplete0(v)
      }

    override final def onComplete[U](func: Try[T] => U)(implicit executor: ExecutionContext): Unit =
      dispatchOrAddCallbacks(new Callback[T](executor.prepare(), func))

    /** Tries to add the callback, if already completed, it dispatches the callback to be executed.
     *  Used by `onComplete()` to add callbacks to a promise and by `link()` to transfer callbacks
     *  to the root promise when linking two promises together.
     */
    @tailrec
    private def dispatchOrAddCallbacks(callbacks: Callbacks[T]): Unit =
      get() match {
        case r: Try[T @unchecked]             => callbacks.submitWithValue(r)
        case dp: DefaultPromise[T @unchecked] => compressedRoot(dp).dispatchOrAddCallbacks(callbacks)
        case cbs: Callbacks[T @unchecked]     => if (compareAndSet(cbs, cbs prepend callbacks)) ()
                                                 else dispatchOrAddCallbacks(callbacks)
      }

    /** Link this promise to the root of another promise using `link()`. Should only be
     *  be called by transformWith.
     */
    protected[concurrent] final def linkRootOf(target: DefaultPromise[T]): Unit = link(target.compressedRoot())

    /** Link this promise to another promise so that both promises share the same
     *  externally-visible state. Depending on the current state of this promise, this
     *  may involve different things. For example, any onComplete listeners will need
     *  to be transferred.
     *
     *  If this promise is already completed, then the same effect as linking -
     *  sharing the same completed value - is achieved by simply sending this
     *  promise's result to the target promise.
     */
    @tailrec
    private def link(target: DefaultPromise[T]): Unit = if (this ne target) {
      get() match {
        case r: Try[T @unchecked] =>
          if (!target.tryComplete(r))
            throw new IllegalStateException("Cannot link completed promises together")
        case dp: DefaultPromise[T @unchecked] =>
          compressedRoot(dp).link(target)
        case cbs: Callbacks[T @unchecked] =>
          if(compareAndSet(cbs, target)) target.dispatchOrAddCallbacks(cbs)
          else link(target)
      }
    }
  }

  /** An already completed Future is given its result at creation.
   *
   *  Useful in Future-composition when a value to contribute is already available.
   */
  final object KeptPromise {
    final def apply[T](result: Try[T]): scala.concurrent.Promise[T] = new DefaultPromise(result)
  }

    /* Encodes the concept of having callbacks.
   * This is an `abstract class` to make sure calls are `invokevirtual` rather than `invokeinterface`
   */
  sealed abstract class Callbacks[+T] {
    /* Logically prepends the callback `c` onto `this` callback */
    def prepend[U >: T](c: Callbacks[U]): Callbacks[U]
    /* Submits the callback function(s) represented by this Callback to be executed with the given value `v` */
    def submitWithValue(v: Try[T @uncheckedVariance]): Unit
  }

  /* Represents 0 Callbacks, is used as an initial, sentinel, value for DefaultPromise
   * This used to be a `case object` but in order to keep `Callbacks`'s methods bimorphic it was reencoded as `val`
   */
  final val NoopCallback: Callbacks[Nothing] =
    new Callback[Nothing](new ExecutionContext {
      override def execute(r: Runnable): Unit = throw new IllegalStateException("Noop ExecutionContext.execute!")
      override def reportFailure(t: Throwable): Unit = t.printStackTrace(System.err)
      override def toString: String = "<Noop>"
    }, _ => ())

  /* Represents a single Callback function.
     Precondition: `executor` is prepar()-ed */
  final class Callback[T](
    final val executor: ExecutionContext,
    final val onComplete: Try[T] => Any) extends Callbacks[T] with Runnable with OnCompleteRunnable {

    private[this] final var value: AnyRef = executor // value is initially the EC

    override final def run(): Unit = value match {
      case null => // already executed once already
      case v: Try[T] =>
        value = null
        try onComplete(v) catch { case NonFatal(e) => executor reportFailure e }
      case e if e eq executor =>
        throw new IllegalStateException("Callback value must be set when running")
    }

    override final def submitWithValue(v: Try[T]): Unit = 
      if (this ne NoopCallback) {
        val e = value
        if (e eq executor) {
          value = v // Safe publication of `value`, to run(), is achieved via `executor.execute(this)`
          // Note that we cannot prepare the ExecutionContext at this point, since we might already be running on a different thread!            
          try executor.execute(this) catch { case NonFatal(t) => executor reportFailure t }
        } // else … already submitted or already executed
      }

    override final def prepend[U >: T](c: Callbacks[U]): Callbacks[U] = c match {
      case a: Callback[U] =>
        if (a eq NoopCallback) this
        else if (this eq NoopCallback) a
        else if (this eq a) this
        else ManyCallbacks.two(this, a)
      case m: ManyCallbacks[U] => m append this // m append this == this prepend m
    }

    override def toString: String = s"Callback($executor, $onComplete)"
  }

  object ManyCallbacks {
    def two[U](second: Callbacks[U], first: Callbacks[U]): ManyCallbacks[U] =
      new ManyCallbacks[U](NoopCallback, NoopCallback, second, first, 2)
    def three[U](third: Callbacks[U], second: Callbacks[U], first: Callbacks[U]): ManyCallbacks[U] =
      new ManyCallbacks[U](NoopCallback, third, second, first, 1)
    def four[U](fourth: Callbacks[U], third: Callbacks[U], second: Callbacks[U], first: Callbacks[U]): ManyCallbacks[U] =
      new ManyCallbacks[U](fourth, third, second, first, 0)
  }

  final class ManyCallbacks[+T] private[ManyCallbacks] (
    final val c1: Callbacks[T],
    final val c2: Callbacks[T],
    final val c3: Callbacks[T],
    final val c4: Callbacks[T],
    private[ManyCallbacks] final val remainingCapacity: Int) extends Callbacks[T] {

    //Don't want to incur the runtime ovehead of these checks, but this invariant will hold true:
    //require(c3 ne NoopCallback)
    //require(c4 ne NoopCallback)

    private[this] final def merge0[U >: T](m: ManyCallbacks[U]): ManyCallbacks[U] =
      (m.remainingCapacity: @switch) match {
        case 0 => ManyCallbacks.two(this, m)
        case 1 => ManyCallbacks.four(c1, c2, c3, ManyCallbacks.four(c4, m.c2, m.c3, m.c4))
        case 2 => ManyCallbacks.three(c1, c2, ManyCallbacks.four(c3, c4, m.c3, m.c4))
      }

    private[this] final def merge1[U >: T](m: ManyCallbacks[U]): ManyCallbacks[U] =
      (m.remainingCapacity: @switch) match {
        case 0 => ManyCallbacks.four(c2, c3, c4, m)
        case 1 => ManyCallbacks.three(c2, c3, ManyCallbacks.four(c4, m.c2, m.c3, m.c4))
        case 2 => ManyCallbacks.two(c2, ManyCallbacks.four(c3, c4, m.c3, m.c4))
      }

    private[this] final def merge2[U >: T](m: ManyCallbacks[U]): ManyCallbacks[U] =
      (m.remainingCapacity: @switch) match {
        case 0 => ManyCallbacks.four(c2, c3, c4, m)
        case 1 => ManyCallbacks.two(c3, ManyCallbacks.four(c4, m.c2, m.c3, m.c4))
        case 2 => ManyCallbacks.four(c3, c4, m.c3, m.c4)
      }

    private[ManyCallbacks] final def merge[U >: T](m: ManyCallbacks[U]): ManyCallbacks[U] =
      if (this ne m) { // Do not merge with itself
        (remainingCapacity: @switch) match {
          case 0 => merge0(m)
          case 1 => merge1(m)
          case 2 => merge2(m)
        }
      } else this

    final def append[U >: T](c: Callbacks[U]): Callbacks[U] = c match {
      case m: ManyCallbacks[U]    => this merge m
      case a if a eq NoopCallback => this // Don't append Noops
      case a: Callback[U]         =>
        (remainingCapacity: @switch) match {
          case 0 => ManyCallbacks.two(this, a)
          case 1 => ManyCallbacks.four(c2, c3, c4, a)
          case 2 => ManyCallbacks.three(c3, c4, a)
        }
    }

    override final def prepend[U >: T](c: Callbacks[U]): Callbacks[U] = c match {
      case m: ManyCallbacks[U]    => m merge this
      case a if a eq NoopCallback => this // Don't prepend Noops
      case a: Callback[U]         =>
        (remainingCapacity: @switch) match {
          case 0 => ManyCallbacks.two(a, this)
          case 1 => ManyCallbacks.four(a, c2, c3, c4)
          case 2 => ManyCallbacks.three(a, c3, c4)
        }
    }

    override final def submitWithValue(v: Try[T @uncheckedVariance]): Unit = {
      c1.submitWithValue(v)
      c2.submitWithValue(v)
      c3.submitWithValue(v)
      c4.submitWithValue(v)
    }

    override final def toString: String = s"Callbacks($c1, $c2, $c3, $c4)"
  }

}
