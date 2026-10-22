package net.dempsy.threading;

import static net.dempsy.utils.test.ConditionPoll.poll;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import net.dempsy.container.Container;
import net.dempsy.container.ContainerJob;
import net.dempsy.container.DummyContainer;
import net.dempsy.container.MessageDeliveryJob;

/**
 * Regression tests for the shutdown path of the {@link OrderedPerContainerThreadingModel}.
 *
 * <p>
 * The container worker threads (named {@code c-<cluster name>}) are non-daemon threads. If
 * {@code close()} doesn't deterministically terminate them the JVM cannot exit and the
 * application layer ends up hard-exiting (e.g. video-processing's "Shutting down hard because
 * after waiting for 10000 MILLISECONDS" followed by a thread dump showing the workers parked
 * in the queue take/poll).
 * </p>
 */
public class TestOrderedPerContainerThreadingModelShutdown {

    private static final String WORKER_THREAD_PREFIX = "c-DummyContainerCluster";

    private static boolean noWorkerThreadsAlive() {
        return Thread.getAllStackTraces().keySet().stream()
            .noneMatch(t -> t.getName().startsWith(WORKER_THREAD_PREFIX) && t.isAlive());
    }

    private static MessageDeliveryJob job(final Container container, final Runnable work) {
        return new MessageDeliveryJob() {

            @Override
            public void rejected(final boolean stopping) {}

            @Override
            public void individuatedJobsComplete() {}

            @Override
            public List<ContainerJob> individuate() {
                return List.of(new ContainerJob() {

                    @Override
                    public void reject(final Container c) {}

                    @Override
                    public void execute(final Container c) {
                        work.run();
                    }
                });
            }

            @Override
            public void executeAllContainers() {
                work.run();
            }

            @Override
            public boolean containersCalculated() {
                return true;
            }

            @Override
            public Container[] containerData() {
                return new Container[] {container};
            }

            @Override
            public void calculateContainers() {}
        };
    }

    @Test
    public void closeTerminatesIdleContainerWorkers() throws Exception {
        final OrderedPerContainerThreadingModel ut = new OrderedPerContainerThreadingModel("ShutdownTest", 1000).start("nodeid");
        try {
            final Container container = new DummyContainer();
            final CountDownLatch ran = new CountDownLatch(1);

            // submitting one job lazily creates the container worker thread.
            ut.submit(job(container, () -> ran.countDown()));
            assertTrue(ran.await(10, TimeUnit.SECONDS));

            // the worker is now idle, parked waiting on its queue.
            assertFalse(noWorkerThreadsAlive());
        } finally {
            ut.close();
        }

        // close() must terminate the (non-daemon) worker thread or the JVM can never exit.
        assertTrue(poll(o -> noWorkerThreadsAlive()));
    }

    @Test
    public void closeTerminatesWorkersEvenWhenInterruptIsSwallowed() throws Exception {
        final OrderedPerContainerThreadingModel ut = new OrderedPerContainerThreadingModel("ShutdownTest", 1000).start("nodeid");
        final Container container = new DummyContainer();

        final CountDownLatch inJob = new CountDownLatch(1);
        final AtomicBoolean sawInterrupt = new AtomicBoolean(false);

        // This job emulates message processor code that swallows the shutdown interrupt:
        // it blocks until it receives an interrupt, swallows it (clearing the flag), and
        // only then returns. The worker thread therefore re-enters its queue wait with no
        // pending interrupt. A blocking take() would park it forever; the bounded poll
        // guarantees it re-checks the stop flag.
        ut.submit(job(container, () -> {
            inJob.countDown();
            while(!sawInterrupt.get()) {
                try {
                    Thread.sleep(10);
                } catch(final InterruptedException ie) {
                    sawInterrupt.set(true); // swallow; interrupt flag is cleared.
                }
            }
        }));

        assertTrue(inJob.await(10, TimeUnit.SECONDS));

        // close() blocks until the workers terminate (bounded), so run it on its own thread.
        final Thread closer = new Thread(() -> ut.close(), "test-closer");
        closer.start();

        closer.join(TimeUnit.SECONDS.toMillis(30));
        assertFalse(closer.isAlive());
        assertTrue(sawInterrupt.get());
        assertTrue(poll(o -> noWorkerThreadsAlive()));
    }
}
