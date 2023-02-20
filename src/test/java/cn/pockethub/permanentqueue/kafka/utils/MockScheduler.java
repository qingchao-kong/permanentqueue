package cn.pockethub.permanentqueue.kafka.utils;

import com.google.common.collect.Ordering;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.common.utils.Time;

import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * A mock scheduler that executes tasks synchronously using a mock time instance. Tasks are executed synchronously when
 * the time is advanced. This class is meant to be used in conjunction with MockTime.
 * <p>
 * Example usage
 * <code>
 * val time = new MockTime
 * time.scheduler.schedule("a task", println("hello world: " + time.milliseconds), delay = 1000)
 * time.sleep(1001) // this should cause our scheduled task to fire
 * </code>
 * <p>
 * Incrementing the time to the exact next execution time of a task will result in that task executing (it as if execution itself takes no time).
 */
public class MockScheduler implements Scheduler {
    private final Time time;

    /* a priority queue of tasks ordered by next execution time */
    private final PriorityQueue<MockTask> tasks = new PriorityQueue<>(MockTask.MockTaskOrdering);

    public MockScheduler(final Time time) {
        this.time = time;
    }

    public Boolean isStarted() {
        return true;
    }


    @Override
    public void startup() {
    }

    public void shutdown() {
        Optional<MockTask> currTask;
        do {
            currTask = poll(mockTask -> true);
            currTask.ifPresent(mockTask -> mockTask.fun.run());
        } while (currTask.isPresent());
    }

    /**
     * Check for any tasks that need to execute. Since this is a mock scheduler this check only occurs
     * when this method is called and the execution happens synchronously in the calling thread.
     * If you are using the scheduler associated with a MockTime instance this call be triggered automatically.
     */
    public void tick() {
        long now = time.milliseconds();
        Optional<MockTask> currTask = Optional.empty();
        /* pop and execute the task with the lowest next execution time if ready */
        do {
            currTask = poll(mockTask -> mockTask.nextExecution <= now);
            currTask.ifPresent(curr -> {
                curr.fun.run();
                /* if the task is periodic, reschedule it and re-enqueue */
                if (curr.isPeriodic()) {
                    curr.nextExecution += curr.period;
                    add(curr);
                }
            });
        } while (currTask.isPresent());
    }

    @Override
    public ScheduledFuture schedule(String name, Runnable fun, long delay, long period, TimeUnit unit) {
        MockTask task = new MockTask(name, fun, time.milliseconds() + delay, period, time);
        add(task);
        tick();
        return task;
    }

    public void clear() {
        synchronized (this) {
            tasks.clear();
        }
    }

    private Optional<MockTask> poll(Predicate<MockTask> predicate) {
        synchronized (this) {
            if (CollectionUtils.isNotEmpty(tasks) && predicate.test(tasks.peek())) {
                return Optional.ofNullable(tasks.poll());
            } else {
                return Optional.empty();
            }
        }
    }

    private void add(MockTask task) {
        synchronized (this) {
            tasks.add(task);
        }
    }

    private static class MockTask implements ScheduledFuture<MockTask> {

        public static final Ordering<MockTask> MockTaskOrdering = new Ordering<MockTask>() {
            @Override
            public int compare(MockTask x, MockTask y) {
                return x.compare(y);
            }
        };

        public final String name;
        public final Runnable fun;
        public long nextExecution;
        public final long period;
        public final Time time;

        public MockTask(final String name, Runnable fun, final long nextExecution, final long period, final Time time) {
            this.name = name;
            this.fun = fun;
            this.nextExecution = nextExecution;
            this.period = period;
            this.time = time;
        }

        public boolean isPeriodic() {
            return period >= 0;
        }

        public int compare(MockTask t) {
            if (t.nextExecution == nextExecution) {
                return 0;
            } else if (t.nextExecution > nextExecution) {
                return -1;
            } else {
                return 1;
            }
        }

        /**
         * Not used, so not not fully implemented
         */
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public MockTask get() {
            return null;
        }

        @Override
        public MockTask get(long timeout, TimeUnit unit) {
            return null;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            synchronized (this) {
                return time.milliseconds() - nextExecution;
            }
        }

        @Override
        public int compareTo(Delayed o) {
            return Long.compare(this.getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
        }
    }
}
