package ro.planet.documentum.stada.modules.pdf;

import com.documentum.fc.client.DfType;
import com.documentum.fc.client.IDfPersistentObject;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfId;
import com.documentum.fc.common.DfTime;
import com.documentum.fc.common.DfUtil;
import com.documentum.fc.common.IDfTime;
import framework.ru.documentum.utils.CalendarHelper;
import framework.ru.documentum.utils.NameUtils;
import framework.ru.documentum.utils.TzCorrectDatesHelper;
import org.apache.commons.lang.StringUtils;
import ro.planet.documentum.stada.common.utils.query.QueryUtils;

import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static ro.planet.documentum.stada.modules.pdf.FieldSpecUtils.split;

public class TemplatePopulateSourceHelper extends DocxHelper {

	private static final String MODIFICATOR_DEFAULT = "default";
	private static final String MODIFICATOR_PREFIX = "prefix";
	private static final String MODIFICATOR_ALTERNATE = "alternate";
	private static final String[] MODIFICATORS = new String[] { MODIFICATOR_DEFAULT, MODIFICATOR_ALTERNATE };
	protected static final String ATTR_NOT_FOUND_ERR = "ATTR_NOT_FOUND";
	protected static final String DOC_DATE_FORMAT_OUT = "dd/MM/yyyy HH:mm:ss";
	protected static final String DOC_DATE_FORMAT_MONTH_RUS = "MONTH_RUS";
	protected ITemplateSource session;

	/**
	 * Список имен всех свойств на момент открытия документа.
	 */
	protected Set<String> currentPropertyNames = new HashSet<>();

	protected String listToString(List<String> items) {
		StringBuilder result = new StringBuilder();
		for (String item : items) {
			if (item.trim().length() == 0) {
				item = "-";
			}
			if (result.length() > 0) {
				result.append(", ");
			}
			result.append(item);
		}
		return result.toString();
	}

	@SuppressWarnings("unchecked")
	protected List<String> checkList(Object obj, boolean canRecurse) {
		List<String> result;
		if (obj instanceof List) {
			result = (List<String>) obj;
		} else {
			result = new ArrayList<>();
			result.add(String.valueOf(obj));
		}
		if (!canRecurse) {
			String item = "";
			if (result.size() > 0) {
				item = result.get(0);
			}
			result.clear();
			result.add(item);
		}
		return result;
	}

	/**
	 * Если object иммет тип IDfTime, то кастит его к IDfTime и возвращает как
	 * результат <br/>
	 * <br/>
	 * <p>
	 * Если object является строкой, представляющей дату в формате
	 * DOC_DATE_FORMAT_OUT, то преобразует ее в дату и возвращает как
	 * результат<br/>
	 * <br/>
	 * <p>
	 * Иначе возвращает NULLDATE
	 */
	private IDfTime checkDate(Object object) {
		if (object instanceof IDfTime) {
			return (IDfTime) object;
		}

		DateFormat dFormatter = new SimpleDateFormat(DOC_DATE_FORMAT_OUT);

		try {
			return new DfTime(dFormatter.parse((String) object));
		} catch (ParseException e) {
			return DfTime.DF_NULLDATE;
		}

	}

	private Integer checkInteger(Object object) {
		if (object instanceof Integer) {
			return (Integer) object;
		} else if (object instanceof String) {
			String str = (String) object;
			if (str.trim().length() == 0) {
				return null;
			}
			return Integer.parseInt(str);
		}

		return null;
	}

	protected String getDateStr(IDfTime time, String format) {
		Date date = time.getDate();
		if (date == null) {
			return "";
		}

		if (DOC_DATE_FORMAT_MONTH_RUS.equals(format)) {
			return getMonthName(time.getMonth());
		}

		DateFormat dateFormat = new SimpleDateFormat(format == null ? DOC_DATE_FORMAT_OUT : format);

		TzCorrectDatesHelper helper = new TzCorrectDatesHelper(session.getDfSession());
		date = helper.correctDateForCurUserTimezone(date);

		return dateFormat.format(date);
	}

	protected FieldName checkFieldNamePrefix(String param) {
		FieldName result = new FieldName(param);
		if (result.getWarning() != null) {
			warning("{0}, {1}", result.getWarning(), param);
		}
		return result;
	}

	protected List<IDfPersistentObject> findFolderRelationChilds(ITemplateSource session,
			IDfPersistentObject caseFolder, String relationName) {
		List<IDfPersistentObject> childObjects = new ArrayList<>();

		try {
			childObjects = session.getChildren(caseFolder, relationName);

		} catch (DfException e) {
			error("Error while searching relation: {0}", e, relationName);
		}
		return childObjects;
	}

	protected String getModificator(String name, String modificator) {
		for (String item : currentPropertyNames) {
			if (item.startsWith(name)) {
				FieldName fieldName = checkFieldNamePrefix(item);
				String data = fieldName.getData();

				if (data.trim().length() > 0) {
					String[] items = split(data, 2);
					trace("Check modificator {0} -> {1}", item, data);

					if (items.length > 1) {
						if (items[0].equalsIgnoreCase(modificator)) {
							String defaultValue = items[1];

							trace("Found modificator {0}, {1} -> {2}", name, modificator, defaultValue);

							return defaultValue;
						}
					}
				}
			}
		}

		return null;
	}

	protected String getRuleByName(String name) {
		for (String item : currentPropertyNames) {
			if (item.startsWith(name)) {
				FieldName fieldName = checkFieldNamePrefix(item);
				String data = fieldName.getData();
				if (data.trim().length() > 0) {
					String[] items = split(data, 2);
					trace("Check modificator {0} -> {1}", item, data);

					if (items.length > 1) {
						boolean found = false;
						for (String key : MODIFICATORS) {
							if (items[0].equalsIgnoreCase(key)) {
								found = true;
							}
						}

						if (!found) {
							trace("Found value {0} -> {1}", name, item);

							return item;
						}
					}
				}
			}
		}

		return null;
	}

	protected Object getNewValue(Object object, String defaultValue) {
		Object newObject = null;

		if (object instanceof List) {
			@SuppressWarnings("unchecked")
			List<String> list = (List<String>) object;
			List<String> newList = new ArrayList<>();
			newList.add(defaultValue);

			trace("Object is a list, {0}, {1} -> {2}", list.size(), Arrays.toString(list.toArray()), defaultValue);

			if (list.size() == 0) {
				newObject = newList;
			}
			if (list.size() == 1) {
				String value = list.get(0);
				if (value.trim().length() == 0) {
					newObject = newList;
				}
			}
		} else if (object instanceof String) {
			trace("Object is a string");

			String value = (String) object;
			if (value.trim().length() == 0) {
				newObject = defaultValue;
			}
		}

		return newObject;
	}

	protected Object setNewValue(Object object, String defaultValue) {
		Object newObject = getNewValue(object, defaultValue);
		if (newObject != null) {
			return newObject;
		}
		return object;
	}

	protected Object getAttrValue(IDfPersistentObject fieldObject, String param, boolean multyResults)
			throws Exception {
		return getAttrValue(fieldObject, param, multyResults, new HashSet<String>());
	}

	private Object getAttrValue(IDfPersistentObject fieldObject, final String param, boolean multyResults,
			Set<String> exclude) throws Exception {
		trace("get attr value, id: {0}, param: {1}, multyResults: {2}, exclude: {3}", fieldObject.getObjectId(), param, multyResults,
				Arrays.toString(exclude.toArray()));

		FieldName fieldName = checkFieldNamePrefix(param);
		String paramData = fieldName.getData();

		Object object;

		if (exclude.contains(paramData)) {
			object = "";
		} else {
			Date start = new Date();
			object = getAttrValue1(fieldObject, paramData, multyResults);
			long delta = new Date().getTime() - start.getTime();
			if (delta > 100) {
				debug("get attr value, id: {0}, param: {1}, multyResults: {2}, exclude: {3}, time: {4}", fieldObject.getObjectId(), param, multyResults,
						Arrays.toString(exclude.toArray()), delta);
			}
		}
		if (object != null) {
			trace("check attr, source: {0}, object: {1}, class: {2}", fieldName.getSource(), object, object.getClass().getName());
		}

		String name = fieldName.getName();
		String defaultValue = getModificator(name, MODIFICATOR_DEFAULT);
		String alternateKey = getModificator(name, MODIFICATOR_ALTERNATE);
		String prefix = getModificator(name, MODIFICATOR_PREFIX);

		if (prefix != null && !prefix.trim().isEmpty()) {
			prefix = prefix.trim() + " ";
		} else {
			prefix = "";
		}

		if (getNewValue(object, "") != null) {

			if (alternateKey != null) {
				String rule = getRuleByName(alternateKey);
				if (rule != null) {
					exclude.add(param);
					return getAttrValue(fieldObject, rule, multyResults, exclude);
				}
			}

			if (defaultValue != null) {
				object = setNewValue(object, defaultValue);
			}
		}

		if (!prefix.isEmpty()) {
			if (object instanceof String && !((String) object).isEmpty()) {
				return prefix + object;
			}

			if (object instanceof List<?>) {
				List<String> newObject = new ArrayList<>();

				for (Object item : (List<?>) object) {
					if (item instanceof String && !((String) item).isEmpty()) {
						newObject.add(prefix + item);
					} else {
						newObject.add((String) item);
					}
				}

				return newObject;
			}
		}

		return object;
	}

	private boolean isTemporaryPositionHistory(IDfPersistentObject fieldObject) {
		try {
			if (fieldObject instanceof IDfSysObject) {
				IDfSysObject sysObject = (IDfSysObject) fieldObject;
				if ("bd_position_history".equalsIgnoreCase(sysObject.getTypeName()) && sysObject.getId("dss_code").isObjectId()) {
					return "temp".equalsIgnoreCase(session.getObject(sysObject.getId("dss_code")).getString("dss_type_appointment"));
				}
			}
		} catch (DfException ex) {
			error("Unable to check if object is temporary position history", ex);
		}
		return false;
	}

	private Object fixPositionValue(Object value, String prefix) {
		debug("Its temporary position history, add prefix '" + prefix + "'");
		if (value instanceof List) {
			@SuppressWarnings("unchecked")
			List<String> result = (List<String>) value;
			for (int i = 0; i < result.size(); i++) {
				if (StringUtils.isNotBlank(result.get(i)))
					result.set(i, prefix + " " + result.get(i));
			}
			return result;
		} else if (value instanceof String && StringUtils.isNotBlank((String) value)) {
			return prefix + " " + value;
		}
		return value;
	}

	private Object getAttrValue1(IDfPersistentObject fieldObject, String param, boolean multyResults) {
		if (param.length() == 0) {
			return "";
		}

		if (fieldObject == null) {
			return ATTR_NOT_FOUND_ERR;
		}

		boolean isTemporaryPositionHistory = isTemporaryPositionHistory(fieldObject);

		if (param.startsWith(ITemplateSource.CUSTOM_ATTRIBUTE_SIGN)) {
			if ("$user_fio_en".equals(param)) {
				String[] items = new String[] { "dss_code.os_ph_user_info.dss_name_native", "dss_performer" };
				for (String item : items) {
					Object result = getAttrValue1(fieldObject, item, false);
					if (getNewValue(result, "") == null) {
						debug("custom attr, param: {0}, result: {1}, item: {2}", param, result, item);
						return result;
					}
				}
				return "";
			}

			if ("$io_lastname".equals(param)) {
				Object oSurname = getSingleValue(
						getAttrValue1(fieldObject, "dss_code.os_ph_user_info.dss_user_last_name", false));
				Object oName = getSingleValue(
						getAttrValue1(fieldObject, "dss_code.os_ph_user_info.dss_user_first_name", false));
				Object oMiddlename = getSingleValue(
						getAttrValue1(fieldObject, "dss_code.os_ph_user_info.dss_user_middle_name", false));

				String iof = "";

				if (oName instanceof String && !((String) oName).isEmpty()) {
					iof = ((String) oName).substring(0, 1).toUpperCase() + ".";
				}

				if (oMiddlename instanceof String && !((String) oMiddlename).isEmpty()) {
					iof += ((String) oMiddlename).substring(0, 1).toUpperCase() + ".";
				}

				if (iof.length() > 0) {
					iof += " ";
				}

				if (oSurname instanceof String && !((String) oSurname).isEmpty()) {
					iof += (String) oSurname;
				}

				return iof;
			}

			if ("$position_en".equals(param)) {
				String[] items = new String[] { "dss_position.dss_name_native", "dss_position.dss_name" };
				for (String item : items) {
					Object result = getAttrValue1(fieldObject, item, false);
					if (getNewValue(result, "") == null) {
						debug("custom attr, param: {0}, result: {1}, item: {2}", param, result, item);
						if (isTemporaryPositionHistory) {
							fixPositionValue(result, "Acting");
						}
						return result;
					}
				}
				return "";
			}

			if ("$position_dativ".equals(param)) {
				String[] items = new String[] { "dss_position.dss_name_dative", "dss_position.dss_name" };
				for (String item : items) {
					Object result = getAttrValue1(fieldObject, item, false);
					if (getNewValue(result, "") == null) {
						debug("custom attr, param: {0}, result: {1}, item: {2}", param, result, item);
						return result;
					}
				}
				return "";
			}

			// ФИО в дательном падеже
			if ("$user_fio_dativ_casus".equals(param)) {
				Object result;
				result = getSingleValue(getAttrValue1(fieldObject, "dss_ext_executor", false));
				Object org = getSingleValue(getAttrValue1(fieldObject, "child_id", false));

				if (result instanceof String && !((String) result).isEmpty() && org instanceof String
						&& DfId.isObjectId((String) org)) {
					try {
						IDfPersistentObject person = session.getDfSession().getObjectByQualification(
								"bd_person WHERE dss_name = '" + result + "' AND FOLDER(ID('" + org + "'))");
						if (person != null && person.hasAttr("dss_name_dativ")) {
							String nameDativ = person.getString("dss_name_dativ");

							if (!nameDativ.trim().isEmpty()) {
								return nameDativ;
							} else {
								return getDativCasusFIO((String) result);
							}
						} else {
							return getDativCasusFIO((String) result);
						}
					} catch (DfException e) {
						return getDativCasusFIO((String) result);
					}
				}

				return "";
			}

			if (param.startsWith("$response_time")) {
				return processResponseTime(fieldObject, param);
			}
		}

		try {
			trace("get attr: {0}, param: {1}", (fieldObject.hasAttr("r_object_id") ? fieldObject.getObjectId() : "X"),
					param);

			String[] params = split(param, 2);

			String relationName;
			if (params.length > 1) {
				relationName = params[0];
			} else {
				relationName = param;
			}
			boolean canRecurse = (relationName.contains(ITemplateSource.FETCH_RECURSIVE_SIGN) || multyResults)
					&& !relationName.contains(ITemplateSource.FETCH_FIRST_SIGN);
			boolean join = relationName.contains(ITemplateSource.FETCH_JOIN_SIGN);

			trace("canRecurse: {0}, join: {1}, params length: {2}", canRecurse, join, params.length);

			if (params.length > 1) {

				String attrName = params[1];
				List<IDfPersistentObject> list = findFolderRelationChilds(session, fieldObject, relationName);
				if (list.size() == 0) {
					return "";
				}

				if (!canRecurse) {
					if (list.size() > 1) {
						list = Arrays.asList(list.get(0));
					}
				}

				List<String> result = new ArrayList<>();
				for (IDfPersistentObject item : list) {

					trace("checking attr with comma: {0}", attrName);

					int commaPos = attrName.indexOf(",");
					int pointPos = attrName.indexOf(".");
					boolean useComma = false;
					if (commaPos > -1) {
						if ((pointPos < 0) || (pointPos > commaPos)) {
							useComma = true;
						}
					}

					String[] attrs;

					if (useComma) {
						attrs = attrName.split(",");
					} else {
						attrs = new String[] { attrName };
					}

					List<String> values = null;

					for (int k = 0; k < attrs.length; k++) {
						trace("checking sub attr {0} for attrName: {1}", attrs[k], attrName);

						List<String> currentValues = checkList(getAttrValue1(item, attrs[k], false), true);
						if (k == 0) {
							values = currentValues;
						} else if (values.size() < currentValues.size()) {
							while (values.size() < currentValues.size()) {
								List<String> newValues = new ArrayList<String>();
								newValues.addAll(values);
								newValues.addAll(values);
								values = newValues;
							}
							while (values.size() > currentValues.size()) {
								values.remove(values.size() - 1);
							}
						}
					}

					trace("checking attr with comma, attrName: {0}, values: {1}, size: {2}", attrName,
							Arrays.toString(values.toArray()), values.size());

					if (isTemporaryPositionHistory && "dss_name".equalsIgnoreCase(attrName)) {
						values = (List<String>) fixPositionValue(values, "И.о.");
					}
					result.addAll(values);
				}
				if (join) {
					return listToString(result);
				}
				return result;
			}

			param = param.replace(ITemplateSource.CURRENT_OBJECT_SIGN, "");
			param = param.replace(ITemplateSource.FETCH_RECURSIVE_SIGN, "");
			param = param.replace(ITemplateSource.FETCH_FIRST_SIGN, "");
			param = param.replace(ITemplateSource.FETCH_JOIN_SIGN, "");

			NameHelper nameHelper = new NameHelper(param, "{", "}");
			param = nameHelper.getFieldName();
			String dateFormat = nameHelper.getValue("format");
			String directory = nameHelper.getValue("sd");

			List<String> items = new ArrayList<>();

			if (fieldObject.hasAttr(param) == false) {
				debug("attribute {0} not found for {1}", param, fieldObject.getObjectId());
				return "";
			}

			int dataType = fieldObject.getAttrDataType(param);
			boolean isRepeating = fieldObject.isAttrRepeating(param);
			debug("dataType: {0}, isRepeating: {1}", dataType, isRepeating);

			if (dataType == DfType.DF_TIME) {
				if (isRepeating) {
					for (int j = 0; j < fieldObject.getValueCount(param); j++) {
						items.add(getDateStr(fieldObject.getRepeatingTime(param, j), dateFormat));
					}
				} else {
					items.add(getDateStr(fieldObject.getTime(param), dateFormat));
				}
			} else {
				if (isRepeating) {
					for (int j = 0; j < fieldObject.getValueCount(param); j++) {
						items.add(fieldObject.getRepeatingString(param, j));
					}
				} else {
					items.add(fieldObject.getString(param));
				}
			}

			processDirectory(items, directory);

			items = checkList(items, canRecurse);

			debug("found items, param: {0}, items: {1}, size: {2}", param, Arrays.toString(items.toArray()),
					items.size());

			if (join) {
				String list = listToString(items);
				return list;
			}

			return items;
		} catch (DfException e) {
			error("unable to get value for param: {0}, return empty string", e, param);
			return "";
		}
	}

	/**
	 * ФИО в дательном падеже
	 */
	public String getDativCasusFIO(String sFio) {
		if (sFio == null || sFio.trim().isEmpty()) {
			return "";
		}

		if (sFio.contains(".")) {
			sFio = sFio.replace(".", ". ");
		}

		String[] fio = sFio.split(" ");
		String surname = "";
		String name = "";
		String middlename = "";

		if (fio.length >= 1) {
			surname = fio[0];
		}

		if (fio.length >= 2) {
			name = fio[1];
		}

		if (fio.length >= 3) {
			middlename = fio[2];
		}

		String gender = NameUtils.guessGender(surname, name, middlename);

		surname = NameUtils.getDativCasusSurname(surname.trim(), gender);
		name = NameUtils.getDativCasusName(name.trim(), gender);
		middlename = NameUtils.getDativCasusMiddlename(middlename.trim(), gender);

		return surname + " " + name + " " + middlename;
	}

	private String processResponseTime(IDfPersistentObject fieldObject, String param) {
		param = param.replace(ITemplateSource.CURRENT_OBJECT_SIGN, "");
		param = param.replace(ITemplateSource.FETCH_RECURSIVE_SIGN, "");
		param = param.replace(ITemplateSource.FETCH_FIRST_SIGN, "");
		param = param.replace(ITemplateSource.FETCH_JOIN_SIGN, "");

		NameHelper nameHelper = new NameHelper(param, "{", "}");
		String fmt = nameHelper.getValue("format");
		SimpleDateFormat dateFormat = new SimpleDateFormat(fmt == null ? DOC_DATE_FORMAT_OUT : fmt);

		FieldName fieldName = checkFieldNamePrefix(param);
		String name = fieldName.getName();

		IDfTime tResponse = checkDate(getSingleValue(getAttrValue1(fieldObject, "dsdt_exec_date", false)));

		if (!tResponse.isNullDate()) {
			return dateFormat.format(tResponse.getDate());
		} else {
			Integer day2complete = checkInteger(getSingleValue(getAttrValue1(fieldObject, "dsi_day2complete", false)));
			IDfTime startDate = checkDate(getSingleValue(getAttrValue1(fieldObject, "dsdt_reg_date", false)));
			Object dayType = getSingleValue(getAttrValue1(fieldObject, "dss_day_unit", false));

			if (day2complete != null) {
				if (startDate.isNullDate()) {
					startDate = new DfTime();
				}

				if (dayType instanceof String) {
					String sDayType = (String) dayType;
					Calendar c = Calendar.getInstance();

					if ("1".equals(sDayType)) { // Календарные
						c.setTime(startDate.getDate());
						c.add(Calendar.DATE, day2complete);
						return dateFormat.format(c.getTime());
					} else if ("0".equals(sDayType)) { // Рабочие
						try {

							int iHoursPerDay = (Integer) getParameterValue("bd", "hoursperday", "int");
							String sWorkCalendarName = (String) getParameterValue("bd", "workcalendarname", "string");

							if (iHoursPerDay > 0 && sWorkCalendarName != null) {
								Date date = new CalendarHelper(session.getDfSession()).adjustDate(startDate.getDate(),
										day2complete * iHoursPerDay, sWorkCalendarName, null);
								return dateFormat.format(date);
							} else {
								return getModificator(name, MODIFICATOR_DEFAULT);
							}
						} catch (DfException e) {
							return getModificator(name, MODIFICATOR_DEFAULT);
						}
					}
				}
			}

			return getModificator(name, MODIFICATOR_DEFAULT);
		}
	}

	private Object getSingleValue(Object obj) {
		if (obj instanceof ArrayList<?>) {
			return ((ArrayList<?>) obj).get(0);
		}
		return obj;
	}

	private void processDirectory(List<String> items, String directory) throws DfException {
		if ((directory != null) && (directory.trim().length() > 0)) {
			for (int i = 0; i < items.size(); i++) {
				String item = items.get(i);
				if (item.trim().length() == 0) {
					continue;
				}

				String query = "select dss_value from bd_simple_directory where dss_directory_type=''{0}'' and dss_code=''{1}''";
				query = MessageFormat.format(query, DfUtil.escapeQuotedString(directory),
						DfUtil.escapeQuotedString(item));
				debug("Directory query {0}", query);

				Date start = new Date();
				String newItem = QueryUtils.getFirstString(session.getDfSession(), query);
				long delta = new Date().getTime() - start.getTime();

				if ((newItem != null) && (newItem.trim().length() > 0)) {
					items.set(i, newItem);
					debug("Update value by directory {0}, {1} -> {2}, time {3} ms", i, item, newItem, delta);

				} else {
					debug("Target not found {0}, {1}, time {2}", i, item, delta);
				}
			}
		}
	}

	private Object getParameterValue(String namespace, String property_name, String valType) {
		String dql = " select property_value from dmc_xcp_app_config where config_type = ''PARAMETER'' AND namespace = ''{0}''"
				+ " AND ANY property_name = ''{1}''" + " AND property_datatype = ''{2}'' "
				+ " ENABLE (ROW_BASED, RETURN_TOP 1)";

		try {
			if ("string".equals(valType)) {
				return QueryUtils.getFirstString(session.getDfSession(),
						MessageFormat.format(dql, namespace, property_name, valType));

			} else if ("int".equals(valType)) {
				return QueryUtils.getFirstInt(session.getDfSession(),
						MessageFormat.format(dql, namespace, property_name, valType));
			}
			return null;

		} catch (DfException e) {
			error("Unable to obtain parameter value for namespace {0} property_name {1}", e, namespace, property_name);
			return null;
		}
	}

	/**
	 * Метод возвращает русское название месяца в родительном падеже по его
	 * номеру
	 */
	private String getMonthName(int iMonth) {
		switch (iMonth) {
		case 1:
			return "Января";
		case 2:
			return "Февраля";
		case 3:
			return "Марта";
		case 4:
			return "Апреля";
		case 5:
			return "Мая";
		case 6:
			return "Июня";
		case 7:
			return "Июля";
		case 8:
			return "Августа";
		case 9:
			return "Сентября";
		case 10:
			return "Октября";
		case 11:
			return "Ноября";
		case 12:
			return "Декабря";
		default:
			return "";
		}
	}

}
