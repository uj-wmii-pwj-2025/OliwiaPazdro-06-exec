package uj.wmii.pwj.exec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

public class MyExecService implements ExecutorService {

    private final BlockingQueue<Runnable> queue;
    private final Thread thread;
    private volatile boolean shutdown = false;
    private volatile boolean terminated = false;

    private MyExecService() {

        queue = new LinkedBlockingQueue<>();
        thread = new Thread(this::runThread);
        thread.start();
    }

    private void runThread() {

        try {
            while (!shutdown || !queue.isEmpty()) {
                Runnable task;
                try {
                    task = queue.take();
                } catch (InterruptedException e) {
                    if (shutdown) {
                        break;
                    } else {
                        continue;
                    }
                }
                task.run();
            }
        } finally {
            terminated = true;
        }
    }

    static MyExecService newInstance() {

        return new MyExecService();
    }

    @Override
    public void shutdown() {

        shutdown = true;
        thread.interrupt();
    }

    @Override
    public List<Runnable> shutdownNow() {

        shutdown = true;
        List<Runnable> copy = new ArrayList<>(queue);
        queue.clear();
        thread.interrupt();
        return copy;
    }

    @Override
    public boolean isShutdown() {

        return shutdown;
    }

    @Override
    public boolean isTerminated() {

        return terminated;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {

        thread.join(unit.toMillis((timeout)));
        return terminated;
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {

        if (task == null)
            throw new NullPointerException("task is null");
        if (shutdown)
            throw new RejectedExecutionException("shutdown");

        FutureTask<T> futureTask = new FutureTask<>(task);
        try {
            queue.put(futureTask);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RejectedExecutionException("interrupted");
        }
        return futureTask;
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {

        if (task == null)
            throw new NullPointerException("task is null");
        if (shutdown)
            throw new RejectedExecutionException("shutdown");

        FutureTask<T> futureTask = new FutureTask<T>(task, result);
        try {
            queue.put(futureTask);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RejectedExecutionException("interrupted");
        }
        return futureTask;
    }

    @Override
    public Future<?> submit(Runnable task) {

        return submit(task, null);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {

        return invokeAll(tasks, Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {

        if (tasks == null || unit == null || tasks.contains(null))
            throw new NullPointerException();

        List<Future<T>> futures = new ArrayList<>(tasks.size());
        long deadline = System.nanoTime() + unit.toNanos(timeout);

        for (Callable<T> task : tasks) {
            Future<T> f = submit(task);
            futures.add(f);
        }

        for (Future<T> f : futures) {
            if (!f.isDone()) {
                try {
                    f.get(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
                } catch (ExecutionException | TimeoutException ignored) {

                }
            }
        }

        for (Future<T> f : futures) {
            if (!f.isDone())
                f.cancel(true);
        }

        return futures;
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {

        try {
            return invokeAny(tasks, Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {

        if (tasks == null || unit == null || tasks.contains(null))
            throw new NullPointerException();
        if (tasks.isEmpty())
            throw new IllegalArgumentException("tasks collection is empty");

        long deadline = System.nanoTime() + unit.toNanos(timeout);
        List<Future<T>> futures = new ArrayList<>(tasks.size());
        ExecutionException lastFailure = null;

        for (Callable<T> task : tasks)
            futures.add(submit(task));

        try {
            for (Future<T> f : futures) {
                long timeLeft = deadline - System.nanoTime();
                if (timeLeft <= 0)
                    throw new TimeoutException("timeout before any task completed successfully");

                try {
                    T result = f.get(timeLeft, TimeUnit.NANOSECONDS);
                    cancelOthers(futures, f);
                    return result;
                } catch (ExecutionException e) {
                    lastFailure = e;
                } catch (TimeoutException te) {
                    f.cancel(true);
                    throw te;
                }
            }

            if (lastFailure != null)
                throw lastFailure;

            throw new ExecutionException(new Exception("no task completed"));

        } finally {
            cancelOthers(futures, null);
        }
    }

    private <T> void cancelOthers(List<Future<T>> futures, Future<T> exclude) {

        for (Future<T> f : futures) {
            if (f != exclude && !f.isDone())
                f.cancel(true);
        }
    }

    @Override
    public void execute(Runnable command) {

        if(command == null)
            throw new NullPointerException("command is null");
        if(shutdown)
            throw new RejectedExecutionException("executor shut down");

        queue.add(command);
    }
}
