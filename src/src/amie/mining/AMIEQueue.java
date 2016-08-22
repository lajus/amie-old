package amie.mining;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A thread-synchronized queue implementation tailored for the AMIE mining system.
 * @author galarrag
 *
 */
public final class AMIEQueue<T> {
	private final Lock lock = new ReentrantLock(); 
	
	private final Condition empty = lock.newCondition(); 
	
	private LinkedHashSet<T> current;
	
	private LinkedHashSet<T> next;
	
	private int generation;
	
	private int maxThreads;
	
	private int waitingThreads = 0;
	
	public AMIEQueue(Collection<T> seeds, int maxThreads) {
		this.generation = 1;
		this.maxThreads = maxThreads; 
		this.waitingThreads = 0;
		this.current = new LinkedHashSet<>(seeds);
		this.next = new LinkedHashSet<>();
	}
	
	public void queue(T o) {
		lock.lock();
		next.add(o);
		lock.unlock();		
	}
	
	public void queueAll(Collection<T> objects) {
		lock.lock();
		next.addAll(objects);
		lock.unlock();				
	}
	
	public T dequeue() throws InterruptedException {
		lock.lock();
		T item = null;
	    if (current.isEmpty()) {
	    	++waitingThreads;
	    	
	    	if (waitingThreads < maxThreads) {
	    		empty.await();
	    	} else {	    	
	    		nextGeneration();
		    	empty.signalAll();	
	    	}
	    	
	    	if (current.isEmpty()) {
	    		item = null;
	    	} else {
	    		item = poll();
	    	}
        } else {
        	item = poll();
        }
		lock.unlock();
	    return item;
	}
	
	private T poll() {
    	Iterator<T> iterator = current.iterator();
        T nextItem = iterator.next();
        iterator.remove();
        return nextItem;		
	}

	private void nextGeneration() {
		generation++;
		current = next;
		next = new LinkedHashSet<>();
	}
	
	public boolean isEmpty() {
		return current.isEmpty() && next.isEmpty();
	}
}
