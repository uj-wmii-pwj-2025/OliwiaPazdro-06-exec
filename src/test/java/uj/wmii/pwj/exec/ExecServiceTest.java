package uj.wmii.pwj.exec;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

public class ExecServiceTest {

    @Test
    void testExecute() {
        MyExecService s = MyExecService.newInstance();
        TestRunnable r = new TestRunnable();
        s.execute(r);
        doSleep(10);
        assertTrue(r.wasRun);
    }

    @Test
    void testScheduleRunnable() {
        MyExecService s = MyExecService.newInstance();
        TestRunnable r = new TestRunnable();
        s.submit(r);
        doSleep(10);
        assertTrue(r.wasRun);
    }

    @Test
    void testScheduleRunnableWithResult() throws Exception {
        MyExecService s = MyExecService.newInstance();
        TestRunnable r = new TestRunnable();
        Object expected = new Object();
        Future<Object> f = s.submit(r, expected);
        doSleep(10);
        assertTrue(r.wasRun);
        assertTrue(f.isDone());
        assertEquals(expected, f.get());
    }

    @Test
    void testScheduleCallable() throws Exception {
        MyExecService s = MyExecService.newInstance();
        StringCallable c = new StringCallable("X", 10);
        Future<String> f = s.submit(c);
        doSleep(300);
        assertTrue(f.isDone());
        assertEquals("X", f.get());
    }

    @Test
    void testShutdown() {
        ExecutorService s = MyExecService.newInstance();
        s.execute(new TestRunnable());
        doSleep(10);
        s.shutdown();
        assertThrows(
            RejectedExecutionException.class,
            () -> s.submit(new TestRunnable()));
    }

    @Test
    void testInvokeAnyCallable() throws Exception {
        MyExecService s = MyExecService.newInstance();
        List<Callable<String>> list = new ArrayList<>();
        list.add(new StringCallable("A", 10));
        list.add(new StringCallable("B", 20));

        String result = s.invokeAny(list);
        assertEquals("A", result);
        s.shutdown();
    }

    @Test
    void testInvokeAnyTimeout() {
        MyExecService s = MyExecService.newInstance();
        List<Callable<String>> list = new ArrayList<>();
        list.add(new StringCallable("slow", 50));

        assertThrows(
                TimeoutException.class,
                () -> s.invokeAny(list, 1, TimeUnit.MILLISECONDS)
        );
        s.shutdown();
    }

    @Test
    void testInvokeAllResults() throws Exception {
        MyExecService s = MyExecService.newInstance();
        List<Callable<String>> list = new ArrayList<>();
        list.add(new StringCallable("X", 5));
        list.add(new StringCallable("Y", 5));

        List<Future<String>> res = s.invokeAll(list, 100, TimeUnit.MILLISECONDS);
        assertEquals("X", res.get(0).get());
        assertEquals("Y", res.get(1).get());
        s.shutdown();
    }

    @Test
    void testInvokeAllTimeoutCancels() throws InterruptedException {
        MyExecService s = MyExecService.newInstance();
        List<Callable<String>> list = new ArrayList<>();
        list.add(new StringCallable("A", 50));
        list.add(new StringCallable("B", 50));

        List<Future<String>> res = s.invokeAll(list, 1, TimeUnit.MILLISECONDS);
        assertTrue(res.get(0).isCancelled());
        assertTrue(res.get(1).isCancelled());
        s.shutdown();
    }

    @Test
    void testShutdownRejectsNewTasks() {
        MyExecService s = MyExecService.newInstance();
        s.shutdown();
        assertThrows(
                RejectedExecutionException.class,
                () -> s.submit(new TestRunnable())
        );
    }

    @Test
    void testAwaitTermination() throws InterruptedException {
        MyExecService s = MyExecService.newInstance();
        s.execute(new TestRunnable());
        s.shutdown();
        boolean finished = s.awaitTermination(100, TimeUnit.MILLISECONDS);
        assertTrue(finished);
        assertTrue(s.isTerminated());
    }

    @Test
    void testShutdownNow() {
        MyExecService s = MyExecService.newInstance();
        TestRunnable r1 = new TestRunnable();
        TestRunnable r2 = new TestRunnable();
        TestRunnable r3 = new TestRunnable();

        s.execute(r1);
        s.execute(r2);
        s.execute(r3);

        List<Runnable> notRun = s.shutdownNow();
        assertFalse(notRun.isEmpty());
        assertTrue(s.isShutdown());
    }

    @Test
    void testSubmitRunnableReturnsNull() throws Exception {
        MyExecService s = MyExecService.newInstance();

        Future<?> f = s.submit(() -> System.out.println("Runnable executed"));

        assertNull(f.get());
        s.shutdown();
    }

    @Test
    void testIsShutdownAndIsTerminated() throws InterruptedException {
        MyExecService s = MyExecService.newInstance();
        s.execute(new TestRunnable());
        assertFalse(s.isShutdown());
        assertFalse(s.isTerminated());

        s.shutdown();
        assertTrue(s.isShutdown());

        boolean finished = s.awaitTermination(200, TimeUnit.MILLISECONDS);
        assertTrue(finished);
        assertTrue(s.isTerminated());
    }




    static void doSleep(int milis) {
        try {
            Thread.sleep(milis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}

class StringCallable implements Callable<String> {

    private final String result;
    private final int milis;

    StringCallable(String result, int milis) {
        this.result = result;
        this.milis = milis;
    }

    @Override
    public String call() throws Exception {
        ExecServiceTest.doSleep(milis);
        return result;
    }
}
class TestRunnable implements Runnable {

    boolean wasRun;
    @Override
    public void run() {
        wasRun = true;
    }
}
