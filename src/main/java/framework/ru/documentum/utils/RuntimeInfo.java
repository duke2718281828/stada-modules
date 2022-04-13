package framework.ru.documentum.utils;

import java.text.MessageFormat;
import java.util.Date;

import com.documentum.fc.client.IDfSession;

import framework.ru.documentum.services.DsHelper;

/**
 * Информация об окружении.
 */
public class RuntimeInfo extends DsHelper {

    /**
     * Конструктор.
     * 
     * @param helper
     *            Объект DsHelper.
     */
    public RuntimeInfo(DsHelper helper) {
	super(helper);
	init();
    }

    /**
     * Всего памяти.
     */
    private long totalMemory;
    /**
     * Свободно памяти.
     */
    private long freeMemory;
    /**
     * Максимально памяти.
     */
    private long maxMemory;

    /**
     * Время создания.
     */
    private Date date;

    /**
     * Конструктор.
     */
    public RuntimeInfo() {
	super((IDfSession) null);
	init();
    }

    /**
     * Инициализация.
     */
    private void init() {
	Runtime rt = Runtime.getRuntime();
	totalMemory = rt.totalMemory();
	freeMemory = rt.freeMemory();
	maxMemory = rt.maxMemory();
	date = new Date();
    }

    /**
     * Преобразование в строку.
     * 
     * @return Строка.
     */
    @Override
    public String toString() {
	return MessageFormat.format("Memory: total {0}, free {1}, used {2}, max {3}", totalMemory, freeMemory,
		totalMemory - freeMemory, maxMemory);
    }

    /**
     * Время создания.
     * 
     * @return Дата.
     */
    public Date getDate() {
	return date;
    }

    /**
     * Информация о разнице между замерянным в начале операции и в настоящее
     * время.
     * 
     * @param initRuntimeInfo
     *            Замер в начале операции.
     */
    public void debugDifference(RuntimeInfo initRuntimeInfo) {
	debug("Init: {0}", initRuntimeInfo.toString());
	debug("Done: {0}", toString());
	long delta = getDate().getTime() - initRuntimeInfo.getDate().getTime();
	debug("Time: {0} seconds", delta / 1000);
    }
}
