package gsa.esg.mekon.core;

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 10/11/2012
 * Time: 23:28
 * To change this template use File | Settings | File Templates.
 */
public interface Environment {
    public Iterable<EventGraphObject> getTriggers(Object function);
    public boolean hasChanged(Object trigger);
    public boolean isInitialised(EventGraphObject trigger);
    public void registerEventSource(EventSource source);
    public void setStickyInGraph(EventGraphObject source, boolean sticky);
    public <T> void addListener(Object source, EventGraphObject sink);
    public <T> void removeListener(Object source, EventGraphObject sink);
    public <T> void addWakeupOrdering(Object source, EventGraphObject wakeupTarget);
    public void wakeupThisCycle(Function target);
    public void fireAfterChangingListeners(Function target);
    public long getEventTime();
    public EventGraphObject getTerminationEvent();
}
