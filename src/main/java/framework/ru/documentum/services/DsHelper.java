package framework.ru.documentum.services;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import com.documentum.fc.client.IDfSession;
import com.documentum.fc.common.DfLogger;

/**
 * Базовый класс для вспомогательных классов-хелперов. Основной смысл данного
 * класса сохранение в переменной объекта IDfSession, чтобы можно было разбивать
 * логику на много методов, не передавая из метода в метода один и тот же объект
 * IDfSession. Кроме этого добавлены методы для записи логов.
 */
public class DsHelper {

	/**
	 * Сессия.
	 */
	protected IDfSession session;

	/**
	 * Конструктор.
	 * 
	 * @param session
	 *            Сессия.
	 */
	public DsHelper(IDfSession session) {
		this.session = session;
	}

	/**
	 * Конструктор.
	 * 
	 * @param session
	 *            Объект DsHelper.
	 */
	public DsHelper(DsHelper helper) {
	    attachHelperState(helper);
	}
	
	/**
	 * Наследует сессию и обработчики сообщений.
	 * @param helper Объект DsHelper.
	 */
	public void attachHelperState(DsHelper helper)
	{
	    this.session = helper.session;
	    attachLoggers(helper);
	};
	
	/**
	 * Добавляет в список обработчиков сообщений
	 * обработчики другого объекта DsHelper.
	 * @param helper Объект DsHelper.
	 */
	private void attachLoggers(DsHelper helper)
	{
	    if (helper.loggers != null)
	    {
		for (IServiceLogger logger : helper.loggers)
		{
		    addServiceLogger(logger);
		}
	    }
	}

	/**
	 * Метод печатает в журнал отладочную информацию. Сервис должен использовать
	 * методы данного класса для печати отладочной информации.
	 * 
	 * @param message
	 *            Сообщение (должно соответствовать правилам
	 *            MessageFormat.format, в частности ' дублируются в виде '').
	 * @param params
	 *            Параметры
	 * 
	 */
	protected void debug(String message,
			Object... params) {
		String string = MessageFormat.format(message, params);
		System.out.println(string);
		DfLogger.debug(this, message, params, null);

		if (enableServiceLoggers) {
			synchronized (enableServiceLoggers) {
				for (IServiceLogger logger : loggers) {
					logger.debug(message, params, null);
				}
			}
		}
	}

	/**
	 * Метод печатает в журнал отладочную информацию. Сервис должен использовать
	 * методы данного класса для печати отладочной информации.
	 * 
	 * @param message
	 *            Сообщение (должно соответствовать правилам
	 *            MessageFormat.format, в частности ' дублируются в виде '').
	 * @param params
	 *            Параметры
	 * @param t
	 *            Объект Throwable, соответствующий ошибке (обычно метод
	 *            вызывается внутри catch).
	 */
	protected void error(String message,
			Throwable tr,
			Object... args) {
		
		String string = MessageFormat.format(message, args);
		System.out.println(string);
		DfLogger.error(this, message, args, tr);
		
		if (tr != null) {
			try {
				tr.printStackTrace();
			} catch (Throwable ex) {
				debug("Cannot print stack trace");
			}
		}
		
		if (enableServiceLoggers) {
			synchronized (enableServiceLoggers) {
				for (IServiceLogger logger : loggers) {
					logger.error(message, args, tr);
				}
			}
		}
	}

	/**
	 * Список объектов IServiceLogger. Если он не пуст, то при записи в лог
	 * сообщения дублируются в каждый из объектов.
	 */
	private List<IServiceLogger> loggers = null;

	/**
	 * В переменную записывается true, если IServiceLogger добавляется через
	 * addServiceLogger.
	 */
	private Boolean enableServiceLoggers = false;

	/**
	 * Добавление IServiceLogger для записи в него логов.
	 * 
	 * @param logger
	 *            объект, реализующий IServiceLogger.
	 */
	public void addServiceLogger(IServiceLogger logger) {
		synchronized (enableServiceLoggers) {
			enableServiceLoggers = true;

			if (loggers == null) {
				loggers = new ArrayList<IServiceLogger>();
			}
			loggers.add(logger);
		}
	}
}
