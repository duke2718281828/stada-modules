package framework.ru.documentum.utils;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.documentum.fc.common.DfLogger;

/**
 * <p>
 * Title:
 * </p>
 * <p>
 * Description: Класс, с набором статических методов форматирования строк.
 * </p>
 * <p>
 * Copyright: Copyright (c) 2003
 * </p>
 * <p>
 * Company:
 * </p>
 * 
 * @version 1.0
 */
@SuppressWarnings("unchecked")
public class StringUtils {

	public static String addSlashes(String str) {
		return str.replaceAll("\\", "\\\\\\\\").replaceAll("\n", "\\\\n")
				.
				// replaceAll("\r", "\\r").replaceAll("\r", "\\r").
				replaceAll("\r", "\\\\r").replaceAll("\"", "\\\\\"")
				.replaceAll("\'", "\\\\\'");
	}

	/**
	 * @deprecated
	 */
	public static String escapeForDQLString(String str) {
		return str.replaceAll("\'", "\'\'");

	}

	/**
	 * @deprecated
	 */
	public static String escapeApostrofs(String str) {
		return str.replaceAll("'", "''");
	}

	public static List listTrim(List li) {
		for (int i = 0; ((li != null) && (i < li.size())); i++) {
			li.set(i, li.get(i).toString().trim());
		}
		return li;
	}

	public static List<String> strSplit(String str, String separator) {

		List<String> list = new ArrayList<String>();

		if (str != null && str.length() != 0) {
			String[] tokens = str.split(separator);
			for (int i = 0; i < tokens.length; i++) {
				list.add(tokens[i]);
			}
		}

		return list;
	}

	/**
	 * возвращает строку, сформированную из списка
	 * 
	 * @param list
	 *            List
	 * @param separator
	 *            String
	 * @return String
	 */
	public static <T> String joinList(List<T> list, String separator) {
		if (list == null)
			return "";

		StringBuffer strResult = new StringBuffer();
		for (int i = 0; i < list.size(); i++) {
			if (i > 0) {
				strResult.append(separator);
			}
			strResult.append(String.valueOf(list.get(i)));
		}
		return strResult.toString();
	}

	/**
	 * возвращает строку, сформированную из списка в обратном направлении
	 * 
	 * @param list
	 *            List
	 * @param separator
	 *            String
	 * @return String
	 */
	public static <T> String joinRevertList(List<T> list, String separator) {
		if (list == null)
			return "";

		StringBuffer strResult = new StringBuffer();
		for (int i = list.size() - 1; i > -1; i--) {
			strResult.append(String.valueOf(list.get(i)));
			if (i > 0) {
				strResult.append(separator);
			}
		}
		return strResult.toString();
	}

	public static String getCommaSeparatedSet(Set set) {
		StringBuffer result = new StringBuffer(0);
		if (set != null) {
			Iterator it = set.iterator();
			while (it.hasNext()) {
				result.append((String) it.next());
				if (it.hasNext()) {
					result.append(",");
				}
			}
		}
		return result.toString();
	}

	public static String getCommaSeparatedSetInApostrophe(Set set) {
		StringBuffer result = new StringBuffer(0);
		if (set != null) {
			Iterator it = set.iterator();
			while (it.hasNext()) {
				result.append("'");
				result.append((String) it.next());
				result.append("'");
				if (it.hasNext()) {
					result.append(",");
				}
			}
		}
		return result.toString();
	}

	public static String readToString(InputStream in) {
		return readToString(in, null);
	}

	public static String readToString(InputStream in, String charSetName) {
		return readToString(in, charSetName, -1);
	}
	
    public static String readToString(InputStream in, String charSetName, int maxRead ) {
    	return readToString(in, charSetName, maxRead, false);
    }

	public static String readToString(InputStream in, String charSetName,
			int maxRead, boolean checkBOM) {
		byte[] BOM = new byte[]{ (byte)0xEF, (byte)0xBB, (byte)0xBF };
		byte[] BOM16 = new byte[]{ (byte)0xFE, (byte)0xFF };
		String charSet = charSetName;
		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			int readedChars = 0;
			int buf = in.read();
			while (buf != -1 && (maxRead == -1 || maxRead >= readedChars)) {
				out.write(buf);
				buf = in.read();
				readedChars++;
			}
            byte[] outBytes=out.toByteArray();
            if (checkBOM && (outBytes.length >= BOM.length))
            {
            	if(outBytes[0]==BOM[0] && outBytes[1]==BOM[1] && outBytes[2]==BOM[2])
            	{               
            		charSet="UTF-8";
            	}
            	if(outBytes[0]==BOM16[0] && outBytes[1]==BOM16[1])
            	{               
            		charSet="UTF-16BE";
            	}
            	if(outBytes[0]==BOM16[1] && outBytes[1]==BOM16[0])
            	{               
            		charSet="UTF-16LE";
            	}
            }
			if (charSet != null && charSet.length() > 0) {
				return new String(out.toByteArray(), charSet);
			} else {
				return new String(out.toByteArray());
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return "";
	}

	public static String getFileNameFromPath(String filePath) {
		if (filePath == null) {
			return null;
		}
		int nameBeginPos = 0;
		int extBeginPost = filePath.length();
		if (filePath.lastIndexOf("\\") != -1) {
			nameBeginPos = filePath.lastIndexOf("\\") + 1;
		} else if (filePath.lastIndexOf("/") != -1) {
			nameBeginPos = filePath.lastIndexOf("/") + 1;
		}
		if (filePath.lastIndexOf(".") != -1) {
			extBeginPost = filePath.lastIndexOf(".");
		}
		filePath = filePath.substring(nameBeginPos, extBeginPost);
		return filePath;
	}

	/**
	 * Возвращает подстроку от последнего вхождения точки "." до конца строки
	 * (Например, расширение файла)
	 * 
	 * @param wholeStr
	 *            String
	 * @return String
	 */
	public static String getSubstringAfterLastPoint(String wholeStr) {
		if (wholeStr == null) {
			return null;
		}
		String strExt = null;
		if (wholeStr.lastIndexOf(".") != -1) {
			int lastPost = wholeStr.length();
			int beginExt = wholeStr.lastIndexOf(".");
			strExt = wholeStr.substring(beginExt + 1, lastPost);
		}
		return strExt;
	}

	/**
	 * Добавляет лидируещие нули (4 штуки).
	 * 
	 * @param number
	 *            long - форматируемое число
	 * @return String
	 */
	public static String getFormatedNumber(long number) {
		return getFormatedNumber(number, "0000");
	}

	/**
	 * Форматирует число в строку по шаблону использую класс DecimalFormat.
	 * например, для формирования числа 00001, pattern = 00000
	 * 
	 * @param number
	 *            long - форматируемое число
	 * @return String
	 */

	public static String getFormatedNumber(long number, String pattern) {
		String result = "";
		NumberFormat format = NumberFormat.getInstance();
		if (format instanceof DecimalFormat) {
			((DecimalFormat) format).applyPattern(pattern);
			result = ((DecimalFormat) format).format(number);
		}
		return result;
	}

	/**
	 * Возращает строку в которой все русские символы замещаются строкой
	 * символов состоящей из &#Unicode; , где Unicode - юникод символа русского
	 * алфавита Для отображеняи русского текста в html-файлах
	 * 
	 * @param sourceStr
	 *            String
	 * @return String
	 */
	public static String getHtmlUnicodeSpecialChars(String sourceStr) {
		StringBuffer buffer = new StringBuffer();
		for (int i = 0; i < sourceStr.length(); i++) {
			char charValue = sourceStr.charAt(i);
			int intValue = charValue;
			if ((intValue < 0x0020) || (intValue > 0x007e)) {
				buffer.append("&#" + intValue + ";");
			} else {
				buffer.append(charValue);
			}
		}
		return buffer.toString();
	}

	public static boolean isEmpty(String str) {
		if (str == null) {
			return true;
		} else {
			return str.length() == 0;
		}
	}

	private static final String UTF8_CHARSET = "UTF-8";

	public static String utf8Trim(String p_Str, int p_MaxLength) {
		int trimTo = p_MaxLength;
		if (p_Str.length() < p_MaxLength) {
			// Строчка короче чем p_MaxLength
			trimTo = p_Str.length();
		}
		String result = p_Str.substring(0, trimTo).trim();
		try {
			while (result.getBytes(UTF8_CHARSET).length > p_MaxLength
					&& result.length() > 0) {
				result = result.substring(0, result.length() - 1);
			}
		} catch (UnsupportedEncodingException ex) {
		}
		return result;
	}

	public static String convertListToString(List<String> list) {
		StringBuffer result = new StringBuffer();

		for (String item : list) {
			if (result.length() > 0)
				result.append(", ");
			result.append(item);
		}

		return result.toString();
	}

}
