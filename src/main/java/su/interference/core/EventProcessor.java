package su.interference.core;

public interface EventProcessor {
    boolean process(Object event);
    boolean delete();
}
