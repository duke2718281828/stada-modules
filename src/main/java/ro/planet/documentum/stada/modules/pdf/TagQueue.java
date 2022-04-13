package ro.planet.documentum.stada.modules.pdf;

import java.util.ArrayList;
import java.util.List;

public class TagQueue<T extends TagBase> {

    private List<T> stack = new ArrayList<>();

    public void pop(T tag) throws Exception {
	for (int i = stack.size() - 1; i >= 0; i--) {
	    T current = stack.get(i);
	    if (current.name.equals(tag.name)) {
		stack.remove(i);
		return;
	    }
	}

	/*
	 * Ошибку не будем возбуждать. Лучше получить хоть какой-то HTML, чем
	 * ничего.
	 */
	
	// throw new Exception("Tag error " + tag.name);
    }

    public int size() {
	return stack.size();
    }

    public void add(T tag) {
	stack.add(tag);
    }

    public T get(int index) {
	return stack.get(index);
    }
}
