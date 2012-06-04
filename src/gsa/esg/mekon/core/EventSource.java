package gsa.esg.mekon.core;

/**
 * A common interface to anything that can provide time cursored sets of data.
 * i.e. both realtime and historic event sources are supported
 * <p/>
 * THREAD SAFTEY:
 * You may only change api-exposed state if:
 * a) you have acquired a lock through the EventManagerInteractor instance that was given to you
 * b) as a result of a call to "advanceState".
 * <p/>
 * User: dvanenckevort_gsa
 * Date: 18-Jan-2007
 * Time: 09:53:24
 */
public interface EventSource extends EventGraphObject {
    /**
     * after calling init, an EventSource should be in a state where hasNext() returns a correct value.
     * the value of currentTime is disregarded until the first "advanceState" has been called.
     *
     * @param startTime
     * @param endTime
     * @return The *current* state of the event source.
     */
    void init(long startTime, long endTime) throws Exception;

    /**
     * Advances the state of this source - ie. applies the next event. This is called by the EventManager
     * at the time specified by getNextTime() if hasNext() returns true.
     * <p/>
     * NOTE: this is always thread safe, you do not need to bother doing any locking.
     */
    void advanceState();

    /**
     * Returns true if there will be no more events (ever) from this source
     *
     * @return true if there will never be any more events from this source
     */
    boolean isComplete();

    /**
     * Returns whether there is more data waiting from this source (at the time returned by getNextTime())
     *
     * @return true if there is more data waiting
     */
    boolean hasNext();

    /**
     * Only makes sense to call this if hasNext() == true
     *
     * @return the point in time represented by the next data element.
     */
    long getNextTime();

    /**
     * "What on earth is this?" you ask. "Why not use a concurrent Condition or something?"
     * Umm, I'll think of a good argument later, but I think I might have a reason.
     * <p/>
     * TODO A couple of java.util.concurrent.Lock objects would do the trick. (fireLock and scheduleLock).
     * TODO init( ... ) might then look something like init( Lock scheduleLock, Lock fireLock, long startTime, long endTime )
     * TODO Unless you've come up with an argument yet...!
     *
     * @param eventManagerInteractor
     */
    void setEventManagerInteractor(EventManagerInteractor eventManagerInteractor);

    public interface EventManagerInteractor {
        /**
         * acquire a lock that allows us to safely change our state (nextTime, hasMore and other implementation state).
         * Implementation should be acquireMutationLock(System.nanoTime())
         */
        void acquireMutationLock();

        /**
         * notifiy the event manager that the event source should be fired as soon as possible (the fire may occur on the
         * current thread)
         */
        void releaseAndFireImmediately();

        /**
         * by releasing this you are telling the event manager that you may have changed your nextState and nextTime
         * information and you may need rescheduling.
         */
        void releaseAndReschedule();

        /**
         * call this to wait until the system has gone realtime.
         * <p/>
         * If the system is never going to reach realtime this will immediately return false,
         * otherwise it will block until realtime then return true
         *
         * @return true if realtime mode has been reached, false if it never will be
         */
        boolean blockUntilRealtime();

        /**
         * I'm experimenting with a better apporach to handling reentrant event sources. this enables the new code paths
         */
        void enableReentrantFire();
    }
}
