package ro.planet.documentum.stada.modules.services.utils.filter;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

public class DayFilter {

    private static HashMap<String, Integer> filters = new HashMap<String, Integer>() {

        /**
         * 
         */
        private static final long serialVersionUID = -7288877564170178409L;

        {
            put("Today", 0);
            put("Last 7 days", -7);
            put("Last 15 days", -15);
            put("Last 30 days", -30);
            put("Last 90 days", -90);
            put("Last half a year", -183);
            put("Last year", -365);
        }
    };

    public static Date getFilterDate(String filterCode) {
        if (filterCode != null && filterCode.length() > 0) {
            return getDay(filters.get(filterCode));
        } else {
            return null;
        }
    }

    private static Date getDay(int days) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, days);
        return cal.getTime();
    }

}
