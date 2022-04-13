package ro.planet.documentum.stada.modules.pdf;

import java.util.HashMap;
import java.util.Map;

public class TagBase {

    /**
     * Имя тега.
     */
    protected String name;

    /**
     * Если тег - самозакрытый, типа <br/>
     * *
     */
    protected boolean closed = false;

    /**
     * Атрибуты.
     */
    protected Map<String, String> attrs = new HashMap<>();

    protected int endPos;

    protected int startPos;
    
    /**
     * Если тег - закрытый, типа
     * </p>
     */
    protected boolean close = false;

    public boolean isNamed(String... values) {
	for (String value : values) {
	    if (name.equalsIgnoreCase(value)) {
		return true;
	    }
	}
	return false;
    }
}
