package ro.planet.documentum.stada.modules.word;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.docx4j.fonts.PhysicalFonts;

import framework.ru.documentum.utils.StringUtils;

public class FontsChecker {

    private void test() throws Exception {
	PhysicalFonts.discoverPhysicalFonts();
	int count = 0;

	Set<String> items = new HashSet<>();

	for (String item : PhysicalFonts.getPhysicalFonts().keySet()) {
	    List<String> l1 = new ArrayList<>();
	    for (String val : item.split(" ")) {
		if (Arrays.asList("bold", "italic", "regular", "demibold", "condensed", "light").contains(val)) {
		    continue;
		}
		l1.add(val);
	    }

	    String newItem = StringUtils.joinList(l1, " ");
	    items.add(newItem);

	}

	for (String item : items) {
	    debug("{0}", item);
	    count++;
	}

	debug("{0} -> {1}", PhysicalFonts.getPhysicalFonts().size(), count);
    }

    protected void debug(String message, Object... params) {
	String string = MessageFormat.format(message, params);
	System.out.println(string);
    }

    public static void main(String[] args) {
	try {
	    new FontsChecker().test();
	} catch (Throwable tr) {
	    throw new RuntimeException(tr);
	}
    }
}
