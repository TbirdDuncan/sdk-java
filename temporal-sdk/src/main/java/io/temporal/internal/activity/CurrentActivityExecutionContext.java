package io.temporal.internal.activity;

import io.temporal.activity.ActivityExecutionContext;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.Collections;

/**
 * Thread-local / virtual-thread-aware store of the context object passed to an activity
 * implementation. Avoid using this class directly.
 *
 * Uses a per-thread stack so nested sets/unsets are handled correctly. Platform threads use
 * ThreadLocal; virtual threads use a WeakHashMap keyed by Thread to avoid leaking memory when
 * virtual threads die.
 *
 * Note: Thread.isVirtual() requires a JDK with virtual thread support (Project Loom).
 *
 * @author fateev (adapted)
 */
final class CurrentActivityExecutionContext {

  // Platform-thread storage: fast and automatic cleanup via ThreadLocal.remove()
  private static final ThreadLocal<Deque<ActivityExecutionContext>> PLATFORM_STACK =
          ThreadLocal.withInitial(ArrayDeque::new);

  // Virtual-thread storage: WeakHashMap so we don't prevent thread GC.
  // Wrapped with synchronizedMap for thread-safety on access.
  private static final Map<Thread, Deque<ActivityExecutionContext>> VIRTUAL_STACKS =
          Collections.synchronizedMap(new WeakHashMap<>());

  private static Deque<ActivityExecutionContext> getStackForCurrentThread() {
    Thread t = Thread.currentThread();
    if (isVirtualThread(t)) {
        Deque<ActivityExecutionContext> d = VIRTUAL_STACKS.computeIfAbsent(t, k -> new ArrayDeque<>());
        return d;
    } else {
      return PLATFORM_STACK.get();
    }
  }

  private static boolean isVirtualThread(Thread t) {
    // Thread.isVirtual() exists in modern JDKs (Project Loom / Java 19+).
    // If you need to compile on older JDKs, replace this call with a compatibility check.
    return t.isVirtual();
  }

  /**
   * This is used by activity implementation to get access to the current ActivityExecutionContext
   */
  public static ActivityExecutionContext get() {
    Deque<ActivityExecutionContext> stack = getStackForCurrentThread();
    ActivityExecutionContext result = stack.peek();
    if (result == null) {
      throw new IllegalStateException(
              "ActivityExecutionContext can be used only inside of activity "
                      + "implementation methods and in the same thread that invoked an activity.");
    }
    return result;
  }

  public static boolean isSet() {
    Deque<ActivityExecutionContext> stack = getStackForCurrentThread();
    return stack.peek() != null;
  }

  /**
   * Pushes the provided context for the current thread. Null context is rejected.
   *
   * We allow nested sets (push semantics) to support nested interceptors / wrappers.
   */
  public static void set(ActivityExecutionContext context) {
    if (context == null) {
      throw new IllegalArgumentException("null context");
    }
    Deque<ActivityExecutionContext> stack = getStackForCurrentThread();
    stack.push(context);
  }

  /**
   * Pops the current context for the thread. If the stack becomes empty, clear the storage for
   * the thread to allow GC (remove ThreadLocal or remove map entry for virtual threads).
   */
  public static void unset() {
    Thread t = Thread.currentThread();
    if (isVirtualThread(t)) {
      synchronized (VIRTUAL_STACKS) {
        Deque<ActivityExecutionContext> stack = VIRTUAL_STACKS.get(t);
        if (stack == null || stack.isEmpty()) {
          // nothing to unset; be lenient
          return;
        }
        stack.pop();
        if (stack.isEmpty()) {
          VIRTUAL_STACKS.remove(t);
        }
      }
    } else {
      Deque<ActivityExecutionContext> stack = PLATFORM_STACK.get();
      if (stack == null || stack.isEmpty()) {
        return;
      }
      stack.pop();
      if (stack.isEmpty()) {
        // remove ThreadLocal entry to avoid holding empty structures
        PLATFORM_STACK.remove();
      }
    }
  }

  private CurrentActivityExecutionContext() {}
}
