package framework.ru.documentum.utils;

import java.text.MessageFormat;

import framework.ru.documentum.services.DsHelper;

import com.documentum.fc.client.DfQuery;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfQuery;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfLogger;

/**
 * Для выполнения DQL запросов.
 */
public class QueryHelper extends DsHelper {

    /**
     * Тип выполнения запроса.
     */
    private int queryType = IDfQuery.DF_QUERY;

    /**
     * Задание типа выполнения запроса.
     * 
     * @param value
     *            Запрос.
     * @return Результат запроса.
     */
    public void setQueryType(int value) {
	queryType = value;
    }

    /**
     * Конструктор.
     * 
     * @param helper
     *            Объект DsHelper.
     */
    public QueryHelper(DsHelper helper) {
	super(helper);
    }

    /**
     * Конструктор.
     * 
     * @param session
     *            Сессия.
     */
    public QueryHelper(IDfSession session) {
	super(session);
    }

    /**
     * Конструктор с заданием типа выполнения запроса.
     * 
     * @param session
     *            Сессия.
     */
    public QueryHelper(IDfSession session, int queryType) {
	super(session);
	this.queryType = queryType;
    }

    /**
     * Метод выполняет запрос. Строка запроса обрабатывается
     * MessageFormat.format.
     * 
     * @param query
     *            Запрос.
     * @param queryProcessor
     *            Объект IQueryProcessor, который используется для обработки
     *            результатов запроса.
     * @param args
     *            Аргументы для формирования запроса.
     * @return Результат запроса.
     */
    public void performArgs(String query, IQueryProcessor queryProcessor, Object... args) throws DfException {
	String actualQuery = MessageFormat.format(query, args);
	perform(actualQuery, queryProcessor);
    }

    /**
     * Метод выполняет запрос.
     * 
     * @param query
     *            Запрос.
     * @param queryProcessor
     *            Объект IQueryProcessor, который используется для обработки
     *            результатов запроса.
     * @return Результат запроса.
     */
    public void perform(String query, IQueryProcessor queryProcessor) throws DfException {

	DfQuery dfquery = new DfQuery();
	dfquery.setDQL(query);
	IDfCollection coll = null;
	try {
	    coll = dfquery.execute(session, queryType);
	    while (coll.next()) {
		if (queryProcessor.process(coll) == false) {
		    break;
		}
	    }
	} catch (DfException x) {
	    DfLogger.error(null, "Cannot perform query {0}", new String[] { query }, x);
	    throw x;
	} finally {
	    try {
		if (coll != null) {
		    coll.close();
		}
	    } catch (DfException ex) {
		DfLogger.error(null, "Error while release session", null, ex);
	    }
	}
    }
}
