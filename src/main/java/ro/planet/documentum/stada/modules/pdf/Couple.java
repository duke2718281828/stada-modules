package ro.planet.documentum.stada.modules.pdf;

public class Couple<T, V> {

    private T first;

    private V second;

    public T getFirst() {
	return first;
    }

    public void setFirst(T first) {
	this.first = first;
    }

    public V getSecond() {
	return second;
    }

    public void setSecond(V second) {
	this.second = second;
    }

    public Couple(T first, V second) {
	this.first = first;
	this.second = second;
    }

}
