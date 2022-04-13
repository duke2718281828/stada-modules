package framework.ru.documentum.utils;

import com.documentum.fc.client.IDfTypedObject;
import com.documentum.fc.common.DfException;

/**
 * 
 * Используется в QueryHelper.
 * 
 */
public interface IQueryProcessor {

	/**
	 * Вызывается для каждой строки результатов запроса.
	 * 
	 * @param obj
	 *            Текущая строка.
	 * @return
	 * @throws DfException
	 */
	public boolean process(IDfTypedObject obj) throws DfException;
}
