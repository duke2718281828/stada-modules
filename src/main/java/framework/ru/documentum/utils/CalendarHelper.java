package framework.ru.documentum.utils;

import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import com.documentum.fc.client.DfClient;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfTime;
import com.documentum.fc.common.IDfId;
import com.documentum.fc.common.IDfTime;
import com.documentum.services.collaboration.ICalendar;
import com.documentum.services.collaboration.ICalendarEvent;
import com.documentum.services.collaboration.ICalendarEventList;
import com.documentum.services.collaboration.ICalendarQuery;
import com.documentum.services.workflowcalendar.IWorkflowCalendar;
import com.documentum.services.workflowcalendar.IWorkflowCalendarEventGroup;
import com.documentum.services.workflowcalendar.IWorkflowCalendarMgmt;

import framework.ru.documentum.services.DsHelper;
import framework.ru.documentum.utils.CalendarExHelper;
import framework.ru.documentum.utils.QueryUtils;

/**
 * Работа с календарем.
 * 
 * 1) Сервис не работает для отрицательных сдвигов.
 * 
 * 
 */


public class CalendarHelper extends DsHelper {
	public CalendarHelper(IDfSession session) throws DfException {
		super(session);
	    }

	    /**
	     * Создает сервис для работы с календарем.
	     * 
	     * См. также https://community.emc.com/message/555307#555307.
	     * 
	     * @return
	     * @throws DfException
	     */
	    public IWorkflowCalendarMgmt getWorkflowCalendarMgr() throws DfException {
		return (IWorkflowCalendarMgmt) DfClient.getLocalClient().newModule(session.getDocbaseName(),
			IWorkflowCalendarMgmt.class.getName(), session.getSessionManager());
	    }

	    public IDfId getCalendar(String name) throws DfException {
		String query = "dmc_calendar where object_name=''{0}''";
		query = MessageFormat.format(query, QueryUtils.makeStringLiteral(name));
		IDfSysObject sys = (IDfSysObject) session.getObjectByQualification(query);
		if (sys == null) {
		    throw new DfException("Calendar not found: " + name);
		}
		return sys.getObjectId();
	    }

	    public void initCalendar(IDfId calendarId, int year) throws DfException {
		Calendar current = Calendar.getInstance();
		current.set(Calendar.YEAR, year);
		current.set(Calendar.MONTH, 0);
		current.set(Calendar.DAY_OF_YEAR, 1);
		current.set(Calendar.HOUR_OF_DAY, 0);
		current.set(Calendar.MINUTE, 0);
		current.set(Calendar.SECOND, 0);
		current.set(Calendar.MILLISECOND, 0);

		Calendar last = Calendar.getInstance();
		last.setTime(current.getTime());
		last.add(Calendar.YEAR, 1);

		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

		while (true) {
		    int dayOfWeek = current.get(Calendar.DAY_OF_WEEK);
		    boolean weekend = (dayOfWeek == Calendar.SATURDAY) || (dayOfWeek == Calendar.SUNDAY);
		    boolean holiday = this.isHoliday(current);

		    Date date = current.getTime();

		    Calendar next = Calendar.getInstance();
		    next.setTime(date);
		    next.add(Calendar.DAY_OF_YEAR, 1);

		    String itemName = format.format(date);

		    if ((weekend == false) && (holiday == false)) {
			String query = "dmc_calendar_event where FOLDER(ID(''{0}'')) and object_name=''{1}''";
			query = MessageFormat.format(query, QueryUtils.makeStringLiteral(calendarId.toString()),
				QueryUtils.makeStringLiteral(itemName));

			IDfSysObject obj = (IDfSysObject) session.getObjectByQualification(query);
			if (obj == null) {
			    obj = (IDfSysObject) session.newObject("dmc_calendar_event");
			    obj.setObjectName(itemName);
			    obj.link(calendarId.toString());
			}

			current.set(Calendar.HOUR_OF_DAY, 23);
			current.set(Calendar.MINUTE, 59);
			current.set(Calendar.SECOND, 59);
			current.set(Calendar.MILLISECOND, 999);

			obj.setTime("start_date", new DfTime(date));
			obj.setTime("end_date", new DfTime(current.getTime()));
			// "Working Day" - используется в IWorkflowCalendarMgmt.
			obj.truncate("keywords", 0);
			obj.appendString("keywords", "Working Day");
			// Нужно, иначе IWorkflowCalendarMgmt не работает.
			obj.setString("event_description", "Default");
			obj.setTime("recurrence_end", new DfTime(last.getTime()));
			obj.save();
			debug("Updated {0}, {1}", date, current.getTime());
		    }

		    current = next;
		    if (current.get(Calendar.YEAR) != year) {
			break;
		    }
		}
	    }

	    /**
	     * Для тестов.
	     * 
	     * @param calendar
	     * @return
	     */
	    private boolean isHoliday(Calendar calendar) {
		int day = calendar.get(Calendar.DAY_OF_MONTH);
		int month = calendar.get(Calendar.MONTH);
		if ((month == Calendar.JANUARY) && (day < 8)) {
		    return true;
		}
		if ((month == Calendar.FEBRUARY) && (day == 23)) {
		    return true;
		}
		if ((month == Calendar.MARCH) && (day == 8)) {
		    return true;
		}
		if ((month == Calendar.MAY) && (day == 1)) {
		    return true;
		}
		if ((month == Calendar.MAY) && (day == 9)) {
		    return true;
		}
		if ((month == Calendar.JUNE) && (day == 12)) {
		    return true;
		}
		if ((month == Calendar.NOVEMBER) && (day == 4)) {
		    return true;
		}
		return false;
	    }

	    public Date adjustDate0(Date start, int hours, String calendarName, String fixedTime) throws DfException {
		if (start == null) {
		    start = new Date();
		}

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(start);
		calendar.add(Calendar.HOUR_OF_DAY, hours);
		Date offsetDefault = calendar.getTime();

		if ((calendarName != null) && (calendarName.trim().length() > 0)) {

		    IWorkflowCalendarMgmt workflowCalendarMgmt = getWorkflowCalendarMgr();

		    if (workflowCalendarMgmt != null) {
			IDfId calendarId = getCalendar(calendarName);
			debug("Start time {0}, calendar {1}", start, calendarId);
			IWorkflowCalendar wfCalendar = workflowCalendarMgmt.getWorkflowCalendar(calendarId);
			debug("Calendar end time {0}", wfCalendar.getEndDate());
			List<IWorkflowCalendarEventGroup> eventGroups = wfCalendar.getEventGroups();
			debug("Event groups {0}", eventGroups.size());
			for (IWorkflowCalendarEventGroup item : eventGroups) {
			    debug("Event group {0}, start {1}, end {2}", item.getName(), item.getStartDate(), item.getEndDate());
			}

			IDfTime resultTime = workflowCalendarMgmt.computeOffSetTime(calendarId, new DfTime(start),
				(long) hours * 60L * 60L);

			debug("Offset time by calendar service {0}, {1}, {2} (default {3})", start, hours,
				resultTime.getDate(), offsetDefault);

			return resultTime.getDate();
		    } else {
			throw new DfException("Cannot obtain workflow calendar service");
		    }

		}

		return offsetDefault;
	    }

	    public Date adjustDate(Date start, int hours, String calendarName, String fixedTime) throws DfException {
		if (start == null) {
		    start = new Date();
		}

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(start);
		calendar.add(Calendar.HOUR_OF_DAY, hours);
		Date offsetDefault = calendar.getTime();

		if ((calendarName != null) && (calendarName.trim().length() > 0)) {

		    IDfId calendarId = getCalendar(calendarName);

		    CalendarExHelper helper = new CalendarExHelper(session);
		    IDfTime resultTime = helper.computeOffSetTime(calendarId, new DfTime(start), (long) hours * 60L * 60L, fixedTime);

		    debug("Offset time by ex calendar {0}, {1}, {2} (default {3})", start, hours, resultTime.getDate(),
			    offsetDefault);

		    return resultTime.getDate();

		}

		return offsetDefault;
	    }

	    
	    public static int getDaysForComputeTimeQuery() {
		return 30;
	    }

	    public static IDfTime getDateAfterDays(IDfTime date, int days) {
		long timeoffset = days * 24 * 60 * 60 * 1000L;
		Date dateAfterDays = new Date(date.getDate().getTime() + timeoffset);
		return new DfTime(dateAfterDays);
	    }

	    private ICalendarEventList getWorkingDays(IDfId calendarId, IDfTime startDate, IDfTime endDate) throws DfException {
		StringBuffer orderByFields = new StringBuffer("r_object_id");
		orderByFields.append(", ");
		orderByFields.append("start_date");

		ICalendar cal = (ICalendar) session.getObject(calendarId);
		ICalendarQuery query = cal.newQuery();
		query.setQualification("ANY keywords='Working Day'");
		query.addAttribute("start_date");
		query.addAttribute("end_date");
		query.setOrderBy(orderByFields.toString());
		query.setDateRange(startDate, endDate, true);
		ICalendarEventList localICalendarEventList = query.execute(session);

		return localICalendarEventList;
	    }

	    public IDfTime computeOffSetTime(IDfId calendarId, IDfTime startDate, long requiredDuration) throws DfException {
		IWorkflowCalendarMgmt service = getWorkflowCalendarMgr();
		IDfTime queryEndDate;
		boolean foundWDaysInCal = false;
		if (requiredDuration == 0L)
		    return startDate;
		if (requiredDuration < 0L) {
		    throw new DfException("INVALID_REQUIRED_DURATION");
		}
		IWorkflowCalendar wfCal = service.getWorkflowCalendar(calendarId);
		if (wfCal == null) {
		    throw new DfException("INVALID_CALENDAR_ID");
		}
		IDfTime calendarEndDate = wfCal.getEndDate();

		if ((startDate == null) || (calendarEndDate.compareTo(startDate) == -1)) {
		    throw new DfException("INVALID_START_DATE");
		}
		long coveredDuration = 0L;

		IDfTime tempEndDate = startDate;
		int daysUsedForLoop = getDaysForComputeTimeQuery();

		IDfTime queryStartDate = new DfTime(startDate.asString("yyyy/mm/dd"));

		debug("Days used for loop {0}", daysUsedForLoop);

		if (daysUsedForLoop != 0)
		    queryEndDate = getDateAfterDays(queryStartDate, daysUsedForLoop);
		else {
		    queryEndDate = calendarEndDate;
		}
		while (queryStartDate.compareTo(calendarEndDate) == -1) {
		    debug("Query events, start {0}, end {1}", queryStartDate, queryEndDate);

		    ICalendarEventList eventList = getWorkingDays(calendarId, queryStartDate, queryEndDate);
		    while (eventList.next()) {
			foundWDaysInCal = true;
			ICalendarEvent event = eventList.getCurrentEvent();
			IDfTime eventStartDate = event.getStartDate();
			IDfTime eventEndDate = event.getEndDate();
			long eventDuration = event.getDuration();

			debug("Process event {0}, {1}, {2} ({3})", eventStartDate, eventEndDate, eventDuration, eventDuration
				/ (24L * 60L * 60L));

			if (eventStartDate.compareTo(tempEndDate) == 1) {
			    tempEndDate = eventStartDate;
			} else if (eventStartDate.compareTo(tempEndDate) == -1) {
			    int compare = eventEndDate.compareTo(tempEndDate);
			    if (compare == 0)
				continue;
			    if (compare == -1) {
				continue;
			    }

			    eventDuration = (eventEndDate.getDate().getTime() - tempEndDate.getDate().getTime()) / 1000L;
			}
			if (coveredDuration + eventDuration == requiredDuration) {
			    return eventEndDate;
			}
			if (coveredDuration + eventDuration > requiredDuration) {
			    long tempEndDateInSecs = tempEndDate.getDate().getTime() / 1000L;
			    Date dueDate = new Date((tempEndDateInSecs + requiredDuration - coveredDuration) * 1000L);
			    return new DfTime(dueDate);
			}

			tempEndDate = eventEndDate;
			coveredDuration += eventDuration;
		    }

		    queryStartDate = queryEndDate;
		    queryEndDate = getDateAfterDays(queryStartDate, daysUsedForLoop);
		}
		if (!(foundWDaysInCal))
		    throw new DfException("NO_WDAYS_IN_CAL");
		throw new DfException("DUEDATE_OUT_OF_CALENDAR_RANGE");
	    }
}
