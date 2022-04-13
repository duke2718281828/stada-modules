package framework.ru.documentum.utils;

import com.documentum.fc.common.*;
import com.documentum.fc.client.*;

import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Vector;

/**
 * 
 * Класс реализует ряд методов для работы с DQL.
 */
public class QueryUtils {

    /**
     * Метод выполняет запрос с одним атрибутом и выдает значения этого атрибута
     * в массиве. использовать в запросах.
     * 
     * @param value
     *            Запрос.
     * @return Результат запроса.
     */
    public static final Vector<String> performStringQuery(String Query, IDfSession session) throws DfException {
	QueryUtils self = new QueryUtils();
	Vector<String> result = new Vector<String>();

	DfQuery dfquery = new DfQuery();
	dfquery.setDQL(Query);
	IDfCollection coll = null;
	try {
	    coll = dfquery.execute(session, IDfQuery.DF_QUERY);
	    String Attr = coll.getAttr(0).getName();
	    while (coll.next()) {
		String s = coll.getString(Attr);
		if (s != null)
		    result.add(s);
	    }
	} catch (DfException x) {
	    DfLogger.error(self, "Cannot perform query {0}", new String[] { Query }, x);
	    throw x;
	} finally {
	    try {
		if (coll != null)
		    coll.close();
	    } catch (DfException ex) {
		DfLogger.error(self, "Error while release session", null, ex);
	    }
	}

	return result;
    }

    public static void executeQuery(String query, IDfSession session) throws DfException {
	QueryUtils self = new QueryUtils();
	DfQuery dfquery = new DfQuery();
	dfquery.setDQL(query);
	IDfCollection coll = null;
	try {
	    coll = dfquery.execute(session, IDfQuery.DF_EXEC_QUERY);
	} catch (DfException x) {
	    DfLogger.error(self, "Cannot perform query {0}", new String[] { query }, x);
	    throw x;
	} finally {
	    try {
		if (coll != null)
		    coll.close();
	    } catch (DfException ex) {
		DfLogger.error(self, "Error while release session", null, ex);
	    }
	}

    }

    /**
     * Метод преобразует строку для того, чтобы ее можно было использовать в
     * запросах.
     * 
     * @param value
     *            Строка.
     * @return Преобразованная строка.
     */
    public static final String makeStringLiteral(String value) {
	StringBuffer result = new StringBuffer();
	for (int i = 0; i < value.length(); i++) {
	    char currentChar = value.charAt(i);
	    if (currentChar == '\'')
		result.append("''");
	    else
		result.append(currentChar);
	}
	return result.toString();
    }

    /**
     * Метод преобразует строку для того, чтобы ее можно было использовать в
     * запросах в условии LIKE.
     * 
     * @param value
     * @return
     */
    public static String makeLikeLiteral(String value) {
	StringBuffer result = new StringBuffer();
	for (int i = 0; i < value.length(); i++) {
	    char currentChar = value.charAt(i);
	    if ((currentChar == '%') || (currentChar == '_') || (currentChar == '\\'))
		result.append("\\" + currentChar);
	    else
		result.append(currentChar);
	}
	return result.toString();
    }

    /**
     * Преобразукт дату в DATE(...) для запроса.
     * 
     * @param date
     * @return
     * @throws Exception
     */
    public static String makeDateLiteral(IDfTime date) throws Exception {

	Date dateValue = date.getDate();
	if (dateValue == null) {
	    dateValue = new Date();
	}
	String javaFormat = "MM/dd/yyyy";
	SimpleDateFormat format = new SimpleDateFormat(javaFormat);
	String dateLiteral = "DATE(''{0}'',''mm/dd/yyyy'')";
	String dateToStr = MessageFormat.format(dateLiteral, format.format(dateValue));
	return dateToStr;
    }

    public static String makeDateLiteral(Date date) throws Exception {
	return makeDateLiteral(new DfTime(date));
    }

    public static String makeDateTimeLiteral(IDfTime date) throws Exception {

	Date dateValue = date.getDate();
	if (dateValue == null) {
	    dateValue = new Date();
	}
	String javaFormat = "MM/dd/yyyy HH:mm:ss";
	SimpleDateFormat format = new SimpleDateFormat(javaFormat);
	String dateLiteral = "DATE(''{0}'',''mm/dd/yyyy hh:mi:ss'')";
	String dateToStr = MessageFormat.format(dateLiteral, format.format(dateValue));
	return dateToStr;
    }

    public static String makeDateTimeLiteral(Date date) throws Exception {
	return makeDateTimeLiteral(new DfTime(date));
    }
    
    public static String performSingleStringQuery(String query, IDfSession session) throws DfException {
	List<String> items = performStringQuery(query, session);
	if (items.size() > 0) {
	    return items.get(0);
	}
	return "";
    }
}
