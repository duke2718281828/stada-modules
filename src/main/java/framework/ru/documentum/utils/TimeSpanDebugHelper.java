package framework.ru.documentum.utils;

import java.util.Date;

import com.documentum.fc.client.IDfSession;

import framework.ru.documentum.services.DsHelper;

public class TimeSpanDebugHelper extends DsHelper {

    private Date start = new Date();

    private long threshold = 1000 * 10;

    private RuntimeInfo initRuntimeInfo = new RuntimeInfo(this);

    public TimeSpanDebugHelper() {
	super((IDfSession) null);
    }

    public boolean complete() {
	Date end = new Date();
	long delta = end.getTime() - start.getTime();
	if (delta > threshold) {

	    new RuntimeInfo(this).debugDifference(initRuntimeInfo);

	    return true;
	}

	return false;
    }
}
