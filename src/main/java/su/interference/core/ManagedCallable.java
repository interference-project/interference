package su.interference.core;

import java.util.concurrent.Callable;

public interface ManagedCallable<T> extends Callable<T> {
    void stop();
}
