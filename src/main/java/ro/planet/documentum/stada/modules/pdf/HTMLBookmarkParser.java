package ro.planet.documentum.stada.modules.pdf;

import java.util.ArrayList;
import java.util.List;

import org.docx4j.finders.RangeFinder;
import org.docx4j.wml.CTBookmark;
import org.docx4j.wml.SdtElement;

public class HTMLBookmarkParser {

    public static final String prefix = "DMSxCP_Content";

    public int parseIndex(String name) {
	if (name.startsWith(prefix) == false) {
	    return -1;
	}

	name = name.substring(prefix.length());
	int index = 0;
	if (name.trim().length() > 0) {
	    index = Integer.parseInt(name);
	}

	return index;
    }

    public int getBookmarkMaxIndex(RangeFinder rt) {
	int index = -1;

	for (CTBookmark bm : rt.getStarts()) {

	    String name = bm.getName();
	    // do we have data for this one?
	    if (name == null)
		continue;

	    int newIndex = parseIndex(name);
	    if (newIndex > index) {
		index = newIndex;
	    }
	}
	return index;
    }

    public int getBookmarkMaxIndex(List<Couple<SdtElement, String>> rt) {
	int index = -1;

	for (Couple<SdtElement, String> bm : rt) {

	    String name = bm.getSecond();
	    // do we have data for this one?
	    if (name == null)
		continue;

	    int newIndex = parseIndex(name);
	    if (newIndex > index) {
		index = newIndex;
	    }
	}
	return index;
    }
}
