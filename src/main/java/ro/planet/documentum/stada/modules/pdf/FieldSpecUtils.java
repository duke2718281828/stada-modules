package ro.planet.documentum.stada.modules.pdf;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FieldSpecUtils {

	public static final String DMS_PROP_RREFIX = "DMSxCP.";
	public static final String DMS_PROP_RREFIX_UTIL = "xCP.";
	public static final String PROP_RREFIX_UTIL = "xCP1.";

	public static String fromRowSpecToSourceSpec(String param) {
		String result = removeLastRowNumber(param);
		if (result.startsWith(DMS_PROP_RREFIX_UTIL)) {
			result = result.substring(DMS_PROP_RREFIX_UTIL.length());
			result = DMS_PROP_RREFIX + result;
		}
		return result;
	}

	public static String removeLastRowNumber(String param) {
		// Remove _N Suffix.
		int p = param.lastIndexOf("_");
		if (p > -1) {
			try {
				String str = param.substring(p + 1);
				Integer.parseInt(str);
				param = param.substring(0, p);
			} catch (Throwable tr) {

			}
		}
		return param;
	}

	public static FieldSpec parseFieldSpec(String value) {
		return FieldSpec.parseFieldSpec(value);
	}

	public static String getFieldSpec(String value) {
		return parseFieldSpec(value).data;
	}

	public static boolean specIsNumber(String spec) {
		if (spec.startsWith("#") == false) {
			return false;
		}
		spec = spec.substring(1);
		if (spec.trim().length() == 0) {
			return true;
		}

		try {
			Integer.parseInt(spec);
		} catch (Throwable tr) {
			return false;
		}
		return true;
	}

	public static String[] split(String str, int max) {
		int c1 = 0;
		int c2 = 0;

		List<String> result = new ArrayList<String>();
		int p = 0;
		for (int i = 0; i < str.length(); i++) {
			char c = str.charAt(i);
			if (c == '{') {
				c1++;
			} else if (c == '}') {
				c1--;
			} else if (c == '[') {
				c2++;
			} else if (c == ']') {
				c2--;
			} else if ((c == '.') && (c1 <= 0) && (c2 <= 0)) {
				int newCount = result.size() + 2;
				if (newCount > max) {
					break;
				}
				result.add(str.substring(p, i));
				p = i + 1;
			}
		}

		result.add(str.substring(p));
		if (result.size() > max) {
			throw new RuntimeException(
					MessageFormat.format("Split error {0}, {1}", str, max, Arrays.toString(result.toArray())));
		}
		return result.toArray(new String[] {});
	}
}
