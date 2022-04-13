package framework.ru.documentum.utils;

import com.documentum.fc.common.DfTime;
import com.documentum.fc.common.IDfTime;
import com.documentum.services.collaboration.IRecurrenceRule;

import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * Исходный класс -
 * com.documentum.services.collaboration.calendar.RecurrenceRule.
 * 
 * 
 * 
 */
public class RecurrenceRule implements IRecurrenceRule {
	private int m_iFrequency = 0;

    private Date m_Until = null;
    private int m_Count = 0;

    private int m_Interval = 1;
    private int[] m_ByMonth = null;
    private String[] m_ByWeekNum = null;

    private int[] m_ByMonthDay = null;
    private int[] m_ByDay = null;

    private int[] m_BySetPos = null;
    private int m_WkStart = 2;

    private DateFormat m_dateFrmt = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'");

    static String[] WEEKDAYS = { "SU", "MO", "TU", "WE", "TH", "FR", "SA" };

    public RecurrenceRule() {
	this.m_dateFrmt.setTimeZone(TimeZone.getTimeZone("GMT"));
    }

    public RecurrenceRule(String paramString) {
	this.m_dateFrmt.setTimeZone(TimeZone.getTimeZone("GMT"));
	fromString(paramString);
    }

    public void fromString(String paramString) {
	String[] arrayOfString = paramString.split(";");

	for (int i = 0; i < arrayOfString.length; ++i) {
	    int j = arrayOfString[i].indexOf(61);
	    String str1 = arrayOfString[i].substring(0, j).toUpperCase();
	    String str2 = arrayOfString[i].substring(j + 1);
	    ParsePosition localParsePosition = new ParsePosition(0);

	    if (str1.equals("FREQ"))
		this.m_iFrequency = parseFrequency(str2.toUpperCase());
	    else if (str1.equals("COUNT"))
		this.m_Count = Integer.parseInt(str2);
	    else if (str1.equals("UNTIL"))
		this.m_Until = this.m_dateFrmt.parse(str2, localParsePosition);
	    else if (str1.equals("INTERVAL"))
		this.m_Interval = Integer.parseInt(str2);
	    else if (str1.equals("BYMONTH"))
		this.m_ByMonth = parseIntegers(str2);
	    else if (str1.equals("BYWEEKNO")) {
		this.m_ByWeekNum = str2.split(",");
	    } else if (str1.equals("BYMONTHDAY"))
		this.m_ByMonthDay = parseMonthDays(str2);
	    else if (str1.equals("BYDAY")) {
		this.m_ByDay = parseByDays(str2);
	    } else if (str1.equals("BYSETPOS"))
		this.m_BySetPos = parseIntegers(str2);
	    else if (str1.equals("WKST"))
		this.m_WkStart = getWeekday(str2);
	}
    }

    public int getCount() {
	return this.m_Count;
    }

    public void setCount(int paramInt) {
	this.m_Count = paramInt;
    }

    public int getFrequency() {
	return this.m_iFrequency;
    }

    public void setFrequency(int paramInt) {
	switch (paramInt) {
	case 1:
	case 2:
	case 3:
	case 6:
	    break;
	case 4:
	case 5:
	default:
	    throw new IllegalArgumentException("Invalid frequency");
	}

	if (this.m_iFrequency == paramInt)
	    return;
	this.m_iFrequency = paramInt;

	this.m_Interval = 1;
	this.m_ByMonth = null;
	this.m_ByWeekNum = null;

	this.m_ByMonthDay = null;
	this.m_ByDay = null;

	this.m_BySetPos = null;
    }

    public int getInterval() {
	return this.m_Interval;
    }

    public void setInterval(int paramInt) {
	this.m_Interval = paramInt;
    }

    public IDfTime getEndDate(IDfTime paramIDfTime) {
	Date localDate = this.m_Until;

	if (this.m_Count > 1) {
	    localDate = computeRecurEnd(paramIDfTime).getDate();
	}

	return new DfTime(localDate);
    }

    public void setEndDate(IDfTime paramIDfTime) {
	this.m_Until = paramIDfTime.getDate();
    }

    public int[] getDaysOfWeek() {
	return this.m_ByDay;
    }

    public void setDaysOfWeek(int[] paramArrayOfInt) {
	if (!(numbersInRange(paramArrayOfInt, 1, 7))) {
	    throw new IllegalArgumentException("Invalid day of week");
	}
	this.m_ByDay = paramArrayOfInt;
    }

    public int[] getDayPositions() {
	return this.m_BySetPos;
    }

    public void setDaysOfWeek(int[] paramArrayOfInt1, int[] paramArrayOfInt2) {
	setDaysOfWeek(paramArrayOfInt1);

	if (!(numbersInRange(paramArrayOfInt2, -366, 366))) {
	    throw new IllegalArgumentException("Invalid day positions");
	}
	this.m_BySetPos = paramArrayOfInt2;
    }

    public int[] getDaysOfMonth() {
	return this.m_ByMonthDay;
    }

    public void setDaysOfMonth(int[] paramArrayOfInt) {
	if (!(numbersInRange(paramArrayOfInt, -31, 31))) {
	    throw new IllegalArgumentException("Invalid days of month");
	}
	this.m_ByMonthDay = paramArrayOfInt;
    }

    public Iterator iterator(IDfTime paramIDfTime) {
	return new Iterator(paramIDfTime);
    }

    public String toString() {
	StringBuffer localStringBuffer = new StringBuffer("FREQ=");
	localStringBuffer.append(formatFrequency());

	if (this.m_Count != 0) {
	    localStringBuffer.append(";COUNT=").append(this.m_Count);
	} else if (this.m_Until != null) {
	    localStringBuffer.append(";UNTIL=").append(this.m_dateFrmt.format(this.m_Until));
	}

	if (this.m_Interval > 0) {
	    localStringBuffer.append(";INTERVAL=").append(this.m_Interval);
	}

	if (this.m_ByMonthDay != null) {
	    localStringBuffer.append(";BYMONTHDAY=");
	    formatNumbers(localStringBuffer, this.m_ByMonthDay);
	}

	if (this.m_ByDay != null) {
	    localStringBuffer.append(";BYDAY=").append(WEEKDAYS[(this.m_ByDay[0] - 1)]);

	    for (int i = 1; i < this.m_ByDay.length; ++i) {
		localStringBuffer.append(',').append(WEEKDAYS[(this.m_ByDay[i] - 1)]);
	    }
	}
	if (this.m_BySetPos != null) {
	    localStringBuffer.append(";BYSETPOS=");
	    formatNumbers(localStringBuffer, this.m_BySetPos);
	}

	return localStringBuffer.toString();
    }

    private int[] parseIntegers(String paramString) {
	String[] arrayOfString = paramString.split(",");
	int[] arrayOfInt = new int[arrayOfString.length];

	for (int i = 0; i < arrayOfString.length; ++i) {
	    arrayOfInt[i] = Integer.parseInt(arrayOfString[i]);
	}

	return arrayOfInt;
    }

    private int[] parseByDays(String paramString) {
	String[] arrayOfString = paramString.split(",");
	int[] arrayOfInt = new int[arrayOfString.length];

	for (int i = 0; i < arrayOfString.length; ++i) {
	    arrayOfInt[i] = getWeekday(arrayOfString[i]);
	}

	return arrayOfInt;
    }

    private int[] parseMonthDays(String paramString) {
	String[] arrayOfString = paramString.split(",");
	int[] arrayOfInt = new int[arrayOfString.length];

	for (int i = 0; i < arrayOfString.length; ++i) {
	    arrayOfInt[i] = Integer.parseInt(arrayOfString[i]);

	    if ((arrayOfInt[i] > 31) || (arrayOfInt[i] < -31)) {
		arrayOfInt[i] = 0;
	    }
	}
	return arrayOfInt;
    }

    private int getWeekday(String paramString) {
	for (int i = 0; i < WEEKDAYS.length; ++i) {
	    if (paramString.indexOf(WEEKDAYS[i]) != -1) {
		return (i + 1);
	    }
	}
	return 0;
    }

    private int parseFrequency(String paramString) {
	int i = 0;
	if (paramString.equals("SECONDLY")) {
	    i = 13;
	} else if (paramString.equals("MINUTELY")) {
	    i = 12;
	} else if (paramString.equals("HOURLY")) {
	    i = 10;
	} else if (paramString.equals("DAILY")) {
	    i = 6;
	} else if (paramString.equals("WEEKLY")) {
	    i = 3;
	} else if (paramString.equals("MONTHLY")) {
	    i = 2;
	} else if (paramString.equals("YEARLY")) {
	    i = 1;
	}
	return i;
    }

    private String formatFrequency() {
	if (this.m_iFrequency == 13) {
	    return "SECONDLY";
	}
	if (this.m_iFrequency == 12) {
	    return "MINUTELY";
	}
	if (this.m_iFrequency == 10) {
	    return "HOURLY";
	}
	if (this.m_iFrequency == 6) {
	    return "DAILY";
	}
	if (this.m_iFrequency == 3) {
	    return "WEEKLY";
	}
	if (this.m_iFrequency == 2) {
	    return "MONTHLY";
	}
	if (this.m_iFrequency == 1) {
	    return "YEARLY";
	}

	return "";
    }

    private boolean numbersInRange(int[] paramArrayOfInt, int paramInt1, int paramInt2) {
	if (paramArrayOfInt == null) {
	    return true;
	}
	for (int i = 0; i < paramArrayOfInt.length; ++i) {
	    if ((paramArrayOfInt[i] > paramInt2) || (paramArrayOfInt[i] < paramInt1)) {
		return false;
	    }
	}
	return true;
    }

    private void formatNumbers(StringBuffer paramStringBuffer, int[] paramArrayOfInt) {
	paramStringBuffer.append(paramArrayOfInt[0]);

	for (int i = 1; i < paramArrayOfInt.length; ++i)
	    paramStringBuffer.append(',').append(paramArrayOfInt[i]);
    }

    private IDfTime computeRecurEnd(IDfTime paramIDfTime) {
	Iterator localIterator = iterator(paramIDfTime);

	while (localIterator.next())
	    ;
	return localIterator.getInstance();
    }

    public static void main(String[] paramArrayOfString) {
	String[] arrayOfString = { "FREQ=DAILY", "FREQ=DAILY;COUNT=4",
		"FREQ=DAILY;BYDAY=MO,TU,WE,TH,FR;UNTIL=20060802T140800Z", "FREQ=DAILY;UNTIL=20060802T140800Z",
		"FREQ=WEEKLY", "FREQ=WEEKLY;COUNT=44", "FREQ=WEEKLY;UNTIL=20060802T140800Z",
		"FREQ=WEEKLY;COUNT=6;BYDAY=MO,WE,FR", "FREQ=WEEKLY;COUNT=6;INTERVAL=2;BYDAY=WE", "FREQ=MONTHLY",
		"FREQ=MONTHLY;COUNT=12", "FREQ=MONTHLY;UNTIL=20060802T140800Z",
		"FREQ=MONTHLY;COUNT=6;BYMONTHDAY=17;WKST=SU", "FREQ=MONTHLY;COUNT=12;BYMONTHDAY=1,2,3;WKST=SU",
		"FREQ=MONTHLY;COUNT=6;INTERVAL=1;BYDAY=WE",
		"FREQ=MONTHLY;COUNT=6;INTERVAL=1;BYDAY=WE;BYSETPOS=2;WKST=SU",
		"FREQ=MONTHLY;COUNT=6;INTERVAL=1;BYDAY=WE;BYSETPOS=2,4;WKST=SU", "FREQ=YEARLY", "FREQ=YEARLY;COUNT=3",
		"FREQ=YEARLY;UNTIL=20060802T140800Z", "FREQ=YEARLY;COUNT=4;BYMONTHDAY=2;BYMONTH=8;WKST=SU" };

	DfTime localDfTime = new DfTime(new Date());
	DateFormat localDateFormat = DateFormat.getDateTimeInstance(0, 2);

	for (int i = 0; i < arrayOfString.length; ++i) {
	    System.out.println("Decoding Recurrence: " + arrayOfString[i]);

	    RecurrenceRule localRecurrenceRule = new RecurrenceRule(arrayOfString[i]);

	    if (localRecurrenceRule.getEndDate(localDfTime).isNullDate()) {
		System.out.println("  Goes on forever.");
	    } else {
		Iterator localIterator = localRecurrenceRule.iterator(localDfTime);

		while (localIterator.next()) {
		    System.out.println("  " + localDateFormat.format(localIterator.getInstance().getDate()));
		}
	    }
	}
    }

    public class Iterator {
	private int m_CurCount;
	private Calendar m_CurInstance;
	private int m_iMonthDay = 0;
	private int m_iDay = 0;
	private int m_iBySetPos = 0;

	public Iterator(IDfTime paramIDfTime) {
	    this.m_CurCount = 0;
	    this.m_CurInstance = Calendar.getInstance();
	    this.m_CurInstance.setTime(paramIDfTime.getDate());

	    if (RecurrenceRule.this.m_ByWeekNum != null) {
		this.m_CurInstance.setMinimalDaysInFirstWeek(4);
	    }
	    this.m_CurInstance.setFirstDayOfWeek(RecurrenceRule.this.m_WkStart);
	}

	public boolean next() {
	    if ((RecurrenceRule.this.m_Count == 0) && (RecurrenceRule.this.m_Until != null)
		    && (this.m_CurInstance.getTime().compareTo(RecurrenceRule.this.m_Until) > 0)) {
		return false;
	    }
	    if ((RecurrenceRule.this.m_Count > 0) && (this.m_CurCount >= RecurrenceRule.this.m_Count)) {
		return false;
	    }

	    if (!(evaluateByRules())) {
		this.m_CurInstance.add(RecurrenceRule.this.m_iFrequency, RecurrenceRule.this.m_Interval);
	    }

	    this.m_CurCount += 1;

	    return ((RecurrenceRule.this.m_Count != 0) || (RecurrenceRule.this.m_Until == null) || (this.m_CurInstance
		    .getTime().compareTo(RecurrenceRule.this.m_Until) <= 0));
	}

	public IDfTime getInstance() {
	    return new DfTime(this.m_CurInstance.getTime());
	}

	private boolean evaluateByRules() {
	    if (RecurrenceRule.this.m_ByMonth != null) {
		this.m_CurInstance.set(2, RecurrenceRule.this.m_ByMonth[0] - 1);
	    }

	    if (RecurrenceRule.this.m_ByMonthDay != null) {
		if (this.m_iMonthDay == RecurrenceRule.this.m_ByMonthDay.length) {
		    this.m_iMonthDay = 0;
		    this.m_CurInstance.add(RecurrenceRule.this.m_iFrequency, RecurrenceRule.this.m_Interval);
		}

		this.m_CurInstance.set(5, RecurrenceRule.this.m_ByMonthDay[(this.m_iMonthDay++)]);
		return true;
	    }

	    if (RecurrenceRule.this.m_ByDay != null) {
		if (this.m_iDay == RecurrenceRule.this.m_ByDay.length) {
		    this.m_iDay = 0;

		    if (RecurrenceRule.this.m_BySetPos == null) {
			this.m_CurInstance.add(3, RecurrenceRule.this.m_Interval);
		    }
		}
		this.m_CurInstance.set(7, RecurrenceRule.this.m_ByDay[(this.m_iDay++)]);

		if (RecurrenceRule.this.m_BySetPos != null) {
		    if (this.m_iBySetPos == RecurrenceRule.this.m_BySetPos.length) {
			this.m_iBySetPos = 0;
			this.m_CurInstance.add(RecurrenceRule.this.m_iFrequency, RecurrenceRule.this.m_Interval);

			this.m_CurInstance.getTime();

			this.m_CurInstance.set(7, RecurrenceRule.this.m_ByDay[(this.m_iDay - 1)]);
		    }

		    int i = this.m_CurInstance.get(2);

		    this.m_CurInstance.set(8, RecurrenceRule.this.m_BySetPos[(this.m_iBySetPos++)]);

		    if ((this.m_iBySetPos == RecurrenceRule.this.m_BySetPos.length) && (i != this.m_CurInstance.get(2))) {
			this.m_iBySetPos = 1;
		    }
		}

		return true;
	    }

	    return false;
	}
    }
}
