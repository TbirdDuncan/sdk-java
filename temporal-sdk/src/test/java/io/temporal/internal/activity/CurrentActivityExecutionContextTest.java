package io.temporal.internal.activity;

import static org.junit.Assert.*;

import io.temporal.activity.ActivityExecutionContext;
import org.junit.Assume;
import org.junit.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicReference;

public class CurrentActivityExecutionContextTest {

    // Create a simple proxy instance that implements the ActivityExecutionContext
    // interface without needing to implement any methods explicitly.
    private static ActivityExecutionContext proxyContext(String name) {
        InvocationHandler handler = (proxy, method, args) -> {
            // no-op; we just want a distinguishable object instance
            return null;
        };
        return (ActivityExecutionContext)
                Proxy.newProxyInstance(
                        ActivityExecutionContext.class.getClassLoader(),
                        new Class[] {ActivityExecutionContext.class},
                        handler);
    }

    @Test
    public void platformThreadNestedSetUnsetBehavior() {
        ActivityExecutionContext ctx1 = proxyContext("ctx1");
        ActivityExecutionContext ctx2 = proxyContext("ctx2");

        // initially not set
        assertFalse(CurrentActivityExecutionContext.isSet());
        assertThrows(
                IllegalStateException.class,
                CurrentActivityExecutionContext::get);

        // set first
        CurrentActivityExecutionContext.set(ctx1);
        assertTrue(CurrentActivityExecutionContext.isSet());
        assertSame(ctx1, CurrentActivityExecutionContext.get(), "should return ctx1");

        // nested set
        CurrentActivityExecutionContext.set(ctx2);
        assertTrue(CurrentActivityExecutionContext.isSet());
        assertSame(ctx2, CurrentActivityExecutionContext.get(), "should return ctx2 (top of stack)");

        // unset should pop back to ctx1
        CurrentActivityExecutionContext.unset();
        assertTrue(CurrentActivityExecutionContext.isSet());
        assertSame(ctx1, CurrentActivityExecutionContext.get(), "after popping, should return ctx1");

        // final unset should clear
        CurrentActivityExecutionContext.unset();
        assertFalse(CurrentActivityExecutionContext.isSet());
        assertThrows(
                IllegalStateException.class,
                CurrentActivityExecutionContext::get,
                "get() should throw after final unset");
    }

    @Test
    void virtualThreadNestedSetUnsetBehavior_ifSupported() throws Exception {
        // detect virtual thread support via presence of Thread.startVirtualThread(Runnable)
        boolean supportsVirtual =
                false;
        try {
            Method m = Thread.class.getMethod("startVirtualThread", Runnable.class);
            supportsVirtual = m != null;
        } catch (NoSuchMethodException e) {
            supportsVirtual = false;
        }

        Assume.assumeTrue( "Virtual threads not supported in this JVM; skipping", supportsVirtual);

        // Use an AtomicReference to capture results from the virtual thread
        AtomicReference<Throwable> failure = new AtomicReference<>(null);
        AtomicReference<ActivityExecutionContext> seenAfterFirstSet = new AtomicReference<>(null);
        AtomicReference<ActivityExecutionContext> seenAfterSecondSet = new AtomicReference<>(null);
        AtomicReference<Boolean> seenIsSetAfterFinalUnset = new AtomicReference<>(null);

        Thread vt =
                Thread.startVirtualThread(
                        () -> {
                            try {
                                ActivityExecutionContext vctx1 = proxyContext("vctx1");
                                ActivityExecutionContext vctx2 = proxyContext("vctx2");

                                // initially should not be set inside the virtual thread
                                assertFalse(CurrentActivityExecutionContext.isSet());
                                try {
                                    CurrentActivityExecutionContext.get();
                                    fail("get() should have thrown when no context is set");
                                } catch (IllegalStateException expected) {
                                }

                                // set first
                                CurrentActivityExecutionContext.set(vctx1);
                                seenAfterFirstSet.set(CurrentActivityExecutionContext.get());

                                // nested set
                                CurrentActivityExecutionContext.set(vctx2);
                                seenAfterSecondSet.set(CurrentActivityExecutionContext.get());

                                // pop once -> back to vctx1
                                CurrentActivityExecutionContext.unset();
                                ActivityExecutionContext afterPop = CurrentActivityExecutionContext.get();
                                if (afterPop != vctx1) {
                                    throw new AssertionError("after pop expected vctx1 but got " + afterPop);
                                }

                                // final unset -> cleared
                                CurrentActivityExecutionContext.unset();
                                seenIsSetAfterFinalUnset.set(CurrentActivityExecutionContext.isSet());
                                // get() should throw now
                                try {
                                    CurrentActivityExecutionContext.get();
                                    throw new AssertionError("get() should have thrown after final unset");
                                } catch (IllegalStateException expected) {
                                }
                            } catch (Throwable t) {
                                failure.set(t);
                            }
                        });

        vt.join();

        if (failure.get() != null) {
            // rethrow assertion failures or other errors from virtual thread
            Throwable t = failure.get();
            if (t instanceof AssertionError) {
                throw (AssertionError) t;
            } else {
                throw new RuntimeException(t);
            }
        }

        // Validate values observed inside the virtual thread
        assertNotNull(seenAfterFirstSet.get(), "virtual thread did not record first set");
        assertNotNull(seenAfterSecondSet.get(), "virtual thread did not record second (nested) set");
        assertFalse(seenIsSetAfterFinalUnset.get(), "expected context to be unset at the end");
    }
}
