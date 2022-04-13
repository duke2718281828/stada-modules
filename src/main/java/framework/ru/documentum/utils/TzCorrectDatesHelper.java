package framework.ru.documentum.utils;

import com.documentum.fc.client.IDfPersistentObject;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfLogger;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import framework.ru.documentum.services.DsHelper;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class TzCorrectDatesHelper extends DsHelper {

    private static final String USER_TIME_ZONE_TYPE = "eifx_user_timezone";
    private static final String ASSUMED_DATE_PATTERN = "dd.MM.yyyy HH:mm:ss";
    // Applicable, as the process in execute(Date[] datesToCorrect)
    // will be applied for multiple dates for the same user.
    private DateTimeZone CACHED_TZ = null;

    public TzCorrectDatesHelper(IDfSession session) {
        super(session);
    }

    public String[] execute(String[] dateStringsToCorrect) {
        debug("Executing TzCorrectDatesHelper#execute(Date[])");
        debug("Converting strings to dates...");

        List<Date> datesToCorrect = stringsToDates(dateStringsToCorrect);
        debug("Conversion successful, starting to apply timezone difference");

        List<Date> correctedDates = new ArrayList<>();
        for (Date dateToCorrect: datesToCorrect) {
            if (dateToCorrect == null) {
                correctedDates.add(null);
                continue;
            }
            if (CACHED_TZ == null) {
                correctedDates.add(correctDateForCurUserTimezone(dateToCorrect));
            } else {
                correctedDates.add(correctDateForTimezone(dateToCorrect, CACHED_TZ));
            }
        }

        debug("Successfully applied timezone difference, converting values back to strings");
        return datesToStrings(correctedDates, dateStringsToCorrect);
    }

    public Date[] execute(Date[] datesToCorrect, DateTimeZone desiredTimezone) {
        debug("Executing TzCorrectDatesHelper#execute(Date[], DateTimeZone), desired timezone = {0}", desiredTimezone.toString());
        Date[] result = new Date[datesToCorrect.length];

        int i = 0;
        for (Date dateToCorrect: datesToCorrect) {
            result[i++] = correctDateForTimezone(dateToCorrect, desiredTimezone);
        }

        return result;
    }

    /**
     * Note: can be called as-is.
     * @param dateToCorrect date in any timezone
     * @return date in current user's timezone (according to data from eifx_user_timezone),
     * or {@code dateToCorrect} if fails
     */
    public Date correctDateForCurUserTimezone(Date dateToCorrect) {
        String username;
        try {
            username = session.getLoginUserName();
            debug("Username for timezone fetch: {0}", username);
            CACHED_TZ = getUserTimeZone(username);
            debug("User timezone = {0}, cached it", CACHED_TZ.toString());

        } catch (DfException e) {
            error("Cannot get current user's name or user timezone: " + e.getMessage(), e);
            return dateToCorrect;
        }

        return correctDateForTimezone(dateToCorrect, CACHED_TZ);
    }

    /**
     * Note: can be called as-is.
     * @param dateToCorrect date in any timezone
     * @param desiredTimezone timezone to apply for the date
     * @return date in {@code desiredTimezone}
     */
    public Date correctDateForTimezone(Date dateToCorrect, DateTimeZone desiredTimezone) {
        LocalDateTime correctedDateTime = new LocalDateTime(dateToCorrect.getTime(), desiredTimezone);
        return correctedDateTime.toDate();
    }

    private DateTimeZone getUserTimeZone(String user) throws DfException {
        DateTimeZone timeZone;
        IDfPersistentObject userZoneInfo = getUserZoneInfo(user);
        if (userZoneInfo == null) {
            userZoneInfo = getUserZoneInfo("default");
        }
        if (userZoneInfo == null) {
            timeZone = DateTimeZone.getDefault();
            DfLogger.warn(this, "No zone info for user {0}, use system default time zone {1} instead",
                            new Object[] { user, timeZone.getID() }, null);
        } else {
            timeZone = DateTimeZone.forTimeZone(TimeZone.getTimeZone(userZoneInfo.getString("time_zone")));
        }

        return timeZone;
    }

    private IDfPersistentObject getUserZoneInfo(String user) throws DfException {
        if (session.getType(USER_TIME_ZONE_TYPE) == null) {
            error("No type eifx_user_timezone to retrieve user timezone info. Stamp date will not be corrected.", new Throwable());
            return null;
        }

        return session.getObjectByQualification(String.format("%s WHERE user_name = '%s'", USER_TIME_ZONE_TYPE, user));
    }


    public static List<Date> stringsToDates(String[] dateStrings) {
        List<Date> result = new ArrayList<>();
        SimpleDateFormat sdf = new SimpleDateFormat(ASSUMED_DATE_PATTERN);

        for (String dateString: dateStrings) {
            if (dateString == null || StringUtils.isEmpty(dateString.trim())
                            || !Character.isDigit(dateString.trim().charAt(0))) {
                // Using null value as a placeholder for unparseable dates.
                result.add(null);
                continue;
            }

            Date date = null;
            try {
                date = sdf.parse(dateString);

            } catch (ParseException e) {
                DfLogger.error(TzCorrectDatesHelper.class, e.getMessage(), null, e);
            }
            result.add(date);
        }

        return result;
    }

    public static String[] datesToStrings(List<Date> correctedDates, String[] originalDates) {
        String[] result = new String[correctedDates.size()];
        SimpleDateFormat sdf = new SimpleDateFormat(ASSUMED_DATE_PATTERN);

        int i = 0;
        for (Date correctedDate: correctedDates) {
            if (correctedDate == null) {
                result[i] = originalDates[i];
                i++;
                continue;
            }
            result[i++] = sdf.format(correctedDate);
        }

        return result;
    }

    /*
    public static void main(String[] args) {
        Date dateToCorrect = new Date();
        DateTimeZone zone = DateTimeZone.forID("Etc/GMT+3");
        LocalDateTime dateTime = new LocalDateTime(dateToCorrect.getTime(), zone);
        Date correctedDate = dateTime.toDate();

        System.out.println(dateToCorrect); // Thu Dec 12 14:36:27 MSK 2019
        System.out.println(correctedDate); // Thu Dec 12 08:36:27 MSK 2019
    }*/

    /*public static void main(String[] args) {
        List<Date> dates = stringsToDates(new String[] {"01.02.2003 12:09:11", "22/01/1996 00:01:00", "invaliddate", "asdf", "12345", null});
        //DateTimeZone timeZone = DateTimeZone.forID("Etc/GMT+3");  // OK
        //timeZone = DateTimeZone.forID("ART");                     // not OK (provider doesn't know ART tz)
        //timeZone = DateTimeZone.forID("Asia/Chita");              // OK after copying tz db from newer version of joda-time
        //DateTimeZone timeZone = DateTimeZone.forTimeZone(TimeZone.getTimeZone("Etc/GMT+3")); // OK
        //timeZone = DateTimeZone.forTimeZone(TimeZone.getTimeZone("ART"));                    // OK
        //timeZone = DateTimeZone.forTimeZone(TimeZone.getTimeZone("Asia/Chita"));             // OK with new version of tz db
        System.out.println();
    }*/

}
