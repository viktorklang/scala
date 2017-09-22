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
import scala.annotation.tailrec
import scala.util.control.NonFatal
import scala.util.{ Try, Success, Failure }

import java.util.concurrent.locks.AbstractQueuedSynchronizer
import java.util.concurrent.atomic.AtomicReference

/* Precondition: `executor` is prepared, i.e., `executor` has been returned from invocation of `prepare` on some other `ExecutionContext`.
 */
private final class CallbackRunnable[T](final val executor: ExecutionContext, final val onComplete: Try[T] => Any) extends Runnable with OnCompleteRunnable {
  private[this] var value: Try[T] = null // must be filled in before running it

  override final def run(): Unit = if (value ne null) // must set value to non-null before running!
    try onComplete(value) catch { case NonFatal(e) => executor reportFailure e }

  /**
   * returns false if this CallbackRunnable already had a value, false if not.
   */
  final def executeWithValue(v: Try[T]): Boolean =
    if (value eq null) {
      value = v
      // Note that we cannot prepare the ExecutionContext at this point, since we might already be running on a different thread!
      try executor.execute(this) catch { case NonFatal(t) => executor reportFailure t }
      true
    } else false
}

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
      case null    => throw new IllegalArgumentException("Cannot complete a Promise with `null`")
      case f: Failure[T @unchecked] => resolveFailure(f)
      case _ => source
    }

  final def transformWithDefaultPromise[T, S](f: Try[T] => Future[S]): DefaultPromise[S] with (Try[T] => Unit) =
   new DefaultPromise[S] with (Try[T] => Unit) {
      private[this] var fun = f
      override final def apply(v: Try[T]): Unit = if (fun ne null) {
        try fun(v) match {
          case fut if fut eq this => complete(Future.coerce(v)) // TODO sensible?
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
   *  1. List[CallbackRunnable] - The promise is incomplete and has zero or more callbacks
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
    def this() = this(Nil)

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
      ready(atMost) // ready throws TimeoutException if timeout so value.get is safe here
      value0.get
    }

    override final def isCompleted: Boolean = value0 ne null

    override def value: Option[Try[T]] = Option(value0)

    @tailrec
    private def value0: Try[T] = get() match {
      case c: Try[T @unchecked] => c
      case dp: DefaultPromise[T @unchecked] => compressedRoot(dp).value0
      case _ => null
    }

    override final def tryComplete(value: Try[T]): Boolean = {
      val resolved = resolveTry(value)
      var listeners = tryCompleteAndGetListeners(resolved)
      if (listeners eq null) false
      else {
         while(listeners ne Nil) {
          val cur = listeners
          listeners = cur.tail
          cur.head.executeWithValue(resolved)
         }
        true
      }
    }

    /** Called by `tryComplete` to store the resolved value and get the list of
     *  listeners, or `null` if it is already completed.
     */
    @tailrec
    private def tryCompleteAndGetListeners(v: Try[T]): List[CallbackRunnable[T]] = {
      get() match {
        case cur: List[CallbackRunnable[T] @unchecked] =>
          if (compareAndSet(cur, v)) cur else tryCompleteAndGetListeners(v)
        case dp: DefaultPromise[T @unchecked] => compressedRoot(dp).tryCompleteAndGetListeners(v)
        case _ => null
      }
    }

    final def onComplete[U](func: Try[T] => U)(implicit executor: ExecutionContext): Unit =
      dispatchOrAddCallback(new CallbackRunnable[T](executor.prepare(), func))

    /** Tries to add the callback, if already completed, it dispatches the callback to be executed.
     *  Used by `onComplete()` to add callbacks to a promise and by `link()` to transfer callbacks
     *  to the root promise when linking two promises together.
     */
    @tailrec
    private def dispatchOrAddCallback(runnable: CallbackRunnable[T]): Unit = {
      get() match {
        case r: Try[T @unchecked]             => runnable.executeWithValue(r)
        case dp: DefaultPromise[T @unchecked] => compressedRoot(dp).dispatchOrAddCallback(runnable)
        case listeners: List[_]               => if (compareAndSet(listeners, runnable :: listeners)) ()
                                                 else dispatchOrAddCallback(runnable)
      }
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
        case listeners: List[CallbackRunnable[T] @unchecked] if compareAndSet(listeners, target) =>
          @tailrec def dispatch(rest: List[CallbackRunnable[T]], target: DefaultPromise[T]): Unit =
            if (rest ne Nil) {
              target.dispatchOrAddCallback(rest.head)
              dispatch(rest.tail, target)
            }
          dispatch(listeners, target)
        case _ =>
          link(target)
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

}
