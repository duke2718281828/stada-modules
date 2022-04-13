package ro.planet.documentum.stada.modules.pdf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.bind.JAXBElement;

import org.apache.commons.lang.StringUtils;
import org.docx4j.TraversalUtil;
import org.docx4j.XmlUtils;
import org.docx4j.model.fields.FieldRef;
import org.docx4j.wml.P;
import org.docx4j.wml.R;

import com.documentum.fc.common.DfLogger;

public class DocxHelper {

	private static final boolean debugApply = false;

	protected class FieldLocator extends TraversalUtil.CallbackImpl {
		public List<Object> objects;
		public Class<?> classObj;

		public FieldLocator(Class<?> classObj) {
			this.classObj = classObj;
			this.objects = new ArrayList<Object>();
		}

		public List<Object> apply(Object o) {
			if (debugApply) {
				debug("{0}", o.getClass().getName());
			}

			if (classObj.isInstance(o)) {
				this.objects.add(o);
			}

			return null;
		}
	}

	protected List<Object> traversalSearch(Object obj, Class<?> classObj) {
		FieldLocator fl = new FieldLocator(classObj);
		new TraversalUtil(obj, fl);
		return fl.objects;
	}

	/**
	 * Оно создает новый R в FieldsPreprocessor.canonicalise, но в нем может не
	 * быть форматирования. Ищем первый попавшийся R и берем из него.
	 * 
	 * @param p
	 * @param fieldRefs
	 * @return
	 */
	protected P canonicalise(P p, List<FieldRef> fieldRefs) {
		P newP = FieldsPreprocessor.canonicalise(p, fieldRefs);
		for (FieldRef ref : fieldRefs) {
			R r = ref.getResultsSlot();
			if (r == null) {
				continue;
			}
			if (r.getRPr() == null) {
				debug("RPr does not specified");

				List<Object> Rs = traversalSearch(p, R.class);
				for (Object item : Rs) {
					if (item != null) {
						R itemR = (R) item;
						if (itemR.getRPr() != null) {
							r.setRPr(itemR.getRPr());
							break;
						}
					}
				}
			}
		}
		return newP;
	}

	protected String marshaltoDebugString(Object obj) {
		if (obj instanceof List) {
			StringBuilder str = new StringBuilder();
			for (Object item : (List) obj) {
				str.append(marshaltoDebugString(item));
				str.append("\r\n");
			}
			return str.toString();
		}
		String result = XmlUtils.marshaltoString(obj, true);
		StringBuilder buffer = new StringBuilder();
		int l = 0;

		for (int i = 0; i < result.length(); i++) {
			char current = result.charAt(i);
			if ((current == '>') || ((current == ' ') && (i - l) > 40)) {
				String str = result.substring(l, i + 1);

				String[] items = str.split("\\r?\\n");
				for (String item : items) {
					if (item.trim().length() > 0) {
						buffer.append(item);
						buffer.append("\r\n");
					}
				}

				l = i + 1;
			}
		}

		buffer.append(result.substring(l));

		return buffer.toString();
	}

	protected String toDebugString(Object... args) {
		return listToDebugString(Arrays.asList(args));
	}

	private String listToDebugString(List values) {
		List<String> items = new ArrayList<String>();
		for (Object item : values) {
			String marshal;
			if (item instanceof List) {
				marshal = listToDebugString((List) item);
			} else if (item instanceof String) {
				marshal = (String) item;
			} else {
				marshal = marshaltoDebugString(item);
			}
			items.add(marshal + "\r\n");
		}
		return "\r\n" + StringUtils.join(items, ",\r\n");
	}

	protected void debug(String message, Object... params) {
		// String string = MessageFormat.format(message, params);
		DfLogger.debug(this, message, params, null);
	}

	protected void trace(String message, Object... params) {
		// String string = MessageFormat.format(message, params);
		DfLogger.debug(this, message, params, null);
	}

	protected void warning(String message, Object... params) {
		message = "warning: " + message;
		debug(message, params);
	}

	protected void error(String message, Throwable tr, Object... params) {
		// String string = MessageFormat.format(message, params);
		DfLogger.error(this, message, params, tr);

		if (tr != null) {
			try {
				tr.printStackTrace();
			} catch (Throwable ex) {
				debug("cannot print stack trace");
			}
		}
	}

	@SuppressWarnings("rawtypes")
	protected Object unwrap(Object item) {
		if (item instanceof JAXBElement) {
			JAXBElement el = (JAXBElement) item;
			item = el.getValue();
		}
		return item;
	}
}
