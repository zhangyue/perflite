package pers.yue.stoppable;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by zhangyue182 on 2019/08/27
 */
public class StoppableFuture<T> {
    private Future<T> future;
    private Stoppable stoppable;

    public StoppableFuture(Future<T> future, Stoppable stoppable) {
        this.future = future;
        this.stoppable = stoppable;
    }

    public Future<T> getFuture() {
        return future;
    }

    public Stoppable getStoppable() {
        return stoppable;
    }

    public void notifyStop() {
        stoppable.notifyStop();
    }

    public T waitForFuture() throws InterruptedException, ExecutionException {
        return future.get();
    }
}
