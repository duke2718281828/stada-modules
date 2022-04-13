package ro.planet.documentum.stada.modules.pdf;

import java.text.MessageFormat;

import com.documentum.fc.client.IDfFormat;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfLogger;

public class ObjectNameHelper {

    private IDfSession session;
    private String formatName;

    public ObjectNameHelper(IDfSession session, String formatName) {
	this.session = session;
	this.formatName = formatName;

    }

    protected void debug(String message, Object... params) {
	String string = MessageFormat.format(message, params);
	System.out.println(string);
	DfLogger.debug(this, message, params, null);
    }

    public String getObjectName(String newObjectName) throws DfException {
	IDfFormat format = session.getFormat(formatName);
	if (format == null) {
	    debug("Cannot obtain format {0}", formatName);
	    return newObjectName;
	}

	debug("Format obtained {0}", formatName);

	String formatExt = format.getDOSExtension();

	debug("Format ext {0}", formatExt);

	if (formatExt.trim().length() > 0) {
	    debug("Check object name {0}", newObjectName);

	    int i = newObjectName.lastIndexOf(".");
	    if (i > -1) {
		String ext = newObjectName.substring(i + 1);

		debug("Current ext {0}", ext);

		if (ext.equalsIgnoreCase(formatExt) == false) {
		    newObjectName = newObjectName.substring(0, i + 1) + formatExt;
		}
	    } else {
		debug("No extension");

		newObjectName += ".";
		newObjectName += formatExt;
	    }

	}

	debug("Object name {0}", newObjectName);
	return newObjectName;
    }

}
