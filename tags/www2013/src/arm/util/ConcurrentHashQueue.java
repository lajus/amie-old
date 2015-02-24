package arm.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;

public class ConcurrentHashQueue<Query> implements Queue<Query>{
	private static final int arraySize = 256;
	
	private Set<Query> subsets[];
	
	private int[] mappings;
	
	
	public ConcurrentHashQueue(int nThreads){
		subsets = new LinkedHashSet[arraySize];
		mappings = new int[nThreads];
		mappings[0] = 0;
		subsets[0] = new LinkedHashSet<>();
	}
	
	public boolean registerThread(long threadId){
		int bucket = (int) (threadId % arraySize);
		if(subsets[bucket] == null){
			subsets[bucket] = new LinkedHashSet<>();
			mappings[(int)(threadId % mappings.length)] = bucket;
			return true;
		}
		
		System.out.println("More than one thread mapping to bucket " + bucket);
		return false;
	}
	
	@Override
	public int size() {
		int bucket = (int) (Thread.currentThread().getId() % arraySize);
		Set<Query> target = subsets[bucket];
		if(subsets[bucket] == null)
			target = subsets[0];

		return target.size();	
	}

	@Override
	public boolean isEmpty() {
		int bucket = (int) (Thread.currentThread().getId() % arraySize);
		Set<Query> target = subsets[bucket];
		if(subsets[bucket] == null)
			target = subsets[0];
		
		return target.isEmpty();	
	}

	@Override
	public boolean contains(Object o) {
		int bucket = (int) (Thread.currentThread().getId() % arraySize);
		Set<Query> target = subsets[bucket];
		if(subsets[bucket] == null)
			target = subsets[0];
		
		synchronized (target) {
			return target.contains(o);	
		}
	}

	@Override
	public Iterator<Query> iterator() {
		int bucket = (int) (Thread.currentThread().getId() % arraySize);
		Set<Query> target = subsets[bucket];
		if(subsets[bucket] == null)
			target = subsets[0];
		
		return target.iterator();
	}

	@Override
	public Object[] toArray() {
		return null;
	}

	@Override
	public <T> T[] toArray(T[] a) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public boolean add(Query e) {
		if(mappings.length == 1){
			synchronized (subsets[0]) {
				return subsets[0].add(e);	
			}
		}else{
			Set<Query> target = null;
			boolean result = false;
			int hc = Math.abs(e.hashCode());
			int bucket = hc % mappings.length;	
			target = subsets[mappings[bucket]];
			
			if(target == null)
				target = subsets[0];
			
			synchronized (target) {
				result = target.add(e);	
			}
			
			return result;
		}
	}

	@Override
	public boolean remove(Object o) {
		return false;
	}
	
	public Query poll(){
		Set<Query> target = null;
		Query result = null;
		if(mappings.length == 1){
			target = subsets[0];
		}else{
			target = subsets[(int) (Thread.currentThread().getId() % arraySize)];	
			if(target == null)
				target = subsets[0];
		}
				
		synchronized (target) {
			if(!target.isEmpty()){
				Iterator<Query> it = target.iterator();
				result = it.next();
				it.remove();
				if(result != null)
					return result;
			}
		}

		if(target != subsets[0]){
			target = subsets[0];
			synchronized (target) {
				if(!target.isEmpty()){
					Iterator<Query> it = target.iterator();
					result = it.next();
					it.remove();
					if(result != null)
						return result;
				}
			}
		}
		
		return result;
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		return false;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void clear() {
	}

	@Override
	public boolean addAll(Collection<? extends Query> c) {
		boolean changed = false;
		if(mappings.length == 1){
			synchronized (subsets[0]) {
				return subsets[0].addAll(c);
			}
		}else{
			for(Query q: c){
				changed = add(q);
			}
		}
		
		return changed;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		return false;

	}

	@Override
	public boolean offer(Query e) {
		// TODO Auto-generated method stub
		return add(e);
	}

	@Override
	public Query remove() {
		// TODO Auto-generated method stub
		Query item = poll();
		if(item == null)
			throw new NoSuchElementException();
		
		return item;
	}

	@Override
	public Query element() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Query peek() {
		// TODO Auto-generated method stub
		return null;
	}
}