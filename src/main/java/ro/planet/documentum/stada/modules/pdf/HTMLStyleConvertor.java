package ro.planet.documentum.stada.modules.pdf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.documentum.fc.client.IDfSession;

import framework.ru.documentum.services.DsHelper;

/**
 * Парсит HTML и заменяет теги вида b, u, font на span+стили.
 *
 * Проверки:
 * 
 * 1) Проверить обработку nowrap в td.
 * 
 * Одиночный nowrap заменяется на nowrap="nowrap" при использовании jtidy.
 */
public class HTMLStyleConvertor extends DsHelper {

	public HTMLStyleConvertor() {
		super((IDfSession) null);

		initTagMappers();
	}

	private boolean skipTables = false;
	private Map<String, TagMapper> tagMappers = new HashMap<>();
	private TagQueue<Tag> stack = new TagQueue<>();
	private TagQueue<Tag> stackAll = new TagQueue<>();

	private class TagMapper {
		private String name;

		private boolean remove = false;

		private Map<String, String> map = new HashMap<>();

		private Map<String, Map<String, String>> valueMap = new HashMap<>();

		private Map<String, String> extra = new HashMap<>();

		public TagMapper(String name, Map<String, String> map, Object... objects) {
			this.name = name;
			this.map = map;
			for (int i = 0; i < objects.length; i++) {
				String key = (String) objects[i];
				i++;
				Map<String, String> value = (Map<String, String>) objects[i];
				valueMap.put(key, value);
			}
		}

	}

	/**
	 * u, font, tags устарели.
	 */
	@SuppressWarnings("serial")
	private void initTagMappers() {
		TagMapper font = new TagMapper("font", new HashMap<String, String>() {
			{
				put("color", "color");
				put("face", "font-family");
				put("size", "font-size");
			}
		}, "size", new HashMap<String, String>() {
			{
				put("1", "10px");
				put("2", "13px");
				put("3", "16px");
				put("4", "18px");
				put("5", "24px");
				put("6", "32px");
				put("7", "48px");
				put("", "20px");
			}
		});

		tagMappers.put(font.name, font);

		TagMapper u = new TagMapper("u", new HashMap<String, String>() {
			{

			}
		});

		u.extra.put("text-decoration", "underline");
		tagMappers.put(u.name, u);

		TagMapper b = new TagMapper("b", new HashMap<String, String>() {
			{

			}
		});

		b.extra.put("font-weight", "bold");
		tagMappers.put(b.name, b);

		TagMapper meta = new TagMapper("meta", new HashMap<String, String>() {
			{

			}
		});
		tagMappers.put(meta.name, meta);
		meta.remove = true;
	}

	public String execute(String text) throws Exception {
		try {
			return execute1(text);
		} catch (Throwable tr) {
			error("style convertor error", tr);
		}
		return text;
	}

	public String execute1(String text) throws Exception {
		StringBuilder result = new StringBuilder();

		text = text.replaceAll("&nbsp;", " ");
		text = text.replaceAll("\0", " ");

		while (true) {
			String newText = text.replaceAll("  ", " ");
			if (newText.length() == text.length()) {
				break;
			}
			text = newText;
		}

		for (int i = 0; i < text.length(); i++) {
			char c = text.charAt(i);
			if (c == '<') {
				Tag tag = parseTag(text, i);

				if (tag.closed == false) {

					/*
					 * Нужно убрать все лишнее, что может быть в таблице. Если
					 * где-то не внутри td есть что-то, например, span внутри
					 * tr, docx4j не может правильно обработать.
					 */
					if (stackAll.size() > 0) {
						Tag current = stackAll.get(stackAll.size() - 1);
						if (current.isNamed("tr", "tbody", "thead", "tfoot", "table")) {
							if ((tag.isNamed("tr", "tbody", "thead", "tfoot", "table", "td", "th") == false)) {
								i = tag.endPos;
								continue;
							}
						}
					}

					if (tag.close) {
						stackAll.pop(tag);
					} else {
						stackAll.add(tag);
					}
				}

				if (skipTag(tag, result)) {
					i = tag.endPos;
					continue;
				}

				if (tag.closed == false) {

					if (tag.close) {

						if (tagMappers.containsKey(tag.name)) {
							// result.append("</span>");
							i = tag.endPos;
							stack.pop(tag);

							if (stack.size() == 0) {
								result.append("</span>");
							}

							continue;
						}
					} else {

						if (tagMappers.containsKey(tag.name)) {
							if (stack.size() > 0) {
								result.append("</span>");
							}
							stack.add(tag);
							result.append("<span style=\"");
							result.append(makeStyle());
							result.append("\">");
							i = tag.endPos;
							continue;
						}
					}
				}

				String text1 = tag.make();
				if (text1 != null) {
					result.append(text1);
					i = tag.endPos;
					continue;
				}

			}
			result.append(c);
		}
		return result.toString();
	}

	private boolean skipTag(Tag tag, StringBuilder builder) {
		String name = tag.name;

		if (tag.isNamed("meta")) {
			return true;
		}
		if (name.contains("@")) {
			return true;
		}

		if (skipTables) {

			if (tag.isNamed("td", "tr", "table", "th")) {
				return true;
			}

			if (tag.isNamed("tbody", "thead", "tfoot")) {
				return true;
			}
		}

		if (tag.isNamed("tbody", "thead", "tfoot")) {
			return true;
		}

		if (tag.isNamed("hr")) {
			return true;
		}

		if (tag.isNamed("br")) {
			builder.append("<br/>");
			return true;
		}
		return false;
	}

	private String makeStyle() {
		StringBuilder result = new StringBuilder();
		for (int i = stack.size() - 1; i >= 0; i--) {
			stack.get(i).add(result);
		}
		return result.toString();
	}

	public class Tag extends TagBase {

		private void fixName() {
			int i = name.indexOf("<");
			if (i > -1) {
				Tag inner = parseTag(name, i);
				debug("inner tag, i: {0}, pos: {1}", i, inner.endPos);
				name = name.substring(0, i) + name.substring(inner.endPos + 1);
			}
		}

		public String make() {
			if (name.startsWith("?")) {
				// XML декларация
				return null;

			}
			if (name.startsWith("!")) {
				// Комментарий <!--
				return null;

			}

			StringBuilder str = new StringBuilder();
			str.append("<");

			if (close) {
				str.append("/");
			}

			fixName();

			str.append(name);
			for (String attr : attrs.keySet()) {
				if (attr.contains("\"")) {
					continue;
				}
				if (attr.contains("\'")) {
					continue;
				}
				String chars = "abcdefghijklmnopqrstuvwxyz:_-*+%#@!~$(){}[]";
				boolean allOK = true;
				String attrLower = attr.toLowerCase();
				for (int i = 0; i < attr.length(); i++) {
					String ch = attrLower.substring(i, i + 1);
					if (chars.contains(ch) == false) {
						allOK = false;
					}
				}
				if (allOK == false) {
					continue;
				}

				str.append(" ");

				String value = attrs.get(attr);
				value = value.replace('"', ' ');
				value = value.replace('\'', ' ');

				str.append(attr);
				str.append("=");
				str.append("\"");
				str.append(value);
				str.append("\"");
			}
			if (closed) {
				str.append("/");
			}
			str.append(">");
			return str.toString();
		}

		private void add(StringBuilder result) {
			TagMapper tagMapper = tagMappers.get(name.toLowerCase());

			for (String attr : attrs.keySet()) {
				if (attr.equals("style")) {
					continue;
				}

				String attrName = tagMapper.map.get(attr);
				if (attrName == null) {
					attrName = attr;
				}
				Map<String, String> valueMap = tagMapper.valueMap.get(attr);

				String value = attrs.get(attr);
				if (valueMap != null) {
					value = valueMap.get(value);
					if (value == null) {
						value = valueMap.get("");
					}
				}

				result.append(attrName);
				result.append(":");
				result.append(value);
				result.append(";");
			}
			for (String attr : tagMapper.extra.keySet()) {
				result.append(attr);
				result.append(":");
				result.append(tagMapper.extra.get(attr));
				result.append(";");
			}

			String style = attrs.get("style");
			if (style != null) {
				result.append(style);
			}
		}
	}

	private int searchEnd(String text, int i, List<Integer> tagStart) {
		int counter = 0;
		int wtFirst = WhitespaceSearcher.search(text);

		for (int k = i; k < text.length(); k++) {
			char ch = text.charAt(k);

			if (k >= wtFirst) {
				/**
				 * Если < > в кавычках, игнорируем. Это считаем значение
				 * атрибута. Но только если это идет после пробела. Т. е. после
				 * имени тега.
				 */
				if (ch == '"') {
					int next = text.indexOf('"', k + 1);
					if (next < 0) {
						break;
					}
					k = next;
					continue;
				}

				if (ch == '\'') {
					int next = text.indexOf('\'', k + 1);
					if (next < 0) {
						break;
					}
					k = next;
					continue;
				}
			}

			if (ch == '<') {
				counter++;
				tagStart.add(k);
			}

			if (ch == '>') {
				counter--;
				if (counter == 0) {
					return k;
				}
			}
		}

		return text.length() - 1;
	}

	/**
	 * Обработка тега вида
	 * 
	 * <</b>span style='font-family: "Times New Roman">
	 * 
	 * @param text
	 * @param i
	 * @param j
	 * @param tagStart
	 * @return
	 */
	private String getTagText(String text, int i, int j, List<Integer> tagStart) {
		List<Tag> innerTags = new ArrayList<>();
		String tagText = text.substring(i, j + 1);
		String initialTagText = text.substring(i + 1, j);

		int endCurrent = i;
		for (Integer tagStartPos : tagStart) {
			if (tagStartPos <= endCurrent) {
				continue;
			}

			int delta = tagStartPos - i;

			Tag inner = parseTag(tagText, delta);
			endCurrent = i + inner.endPos;
			innerTags.add(inner);
		}
		for (int k = innerTags.size() - 1; k >= 0; k--) {
			Tag inner = innerTags.get(k);
			tagText = tagText.substring(0, inner.startPos) + tagText.substring(inner.endPos + 1);
		}

		tagText = tagText.substring(1, tagText.length() - 1);
		tagText = tagText.trim();

		if (initialTagText.equals(tagText) == false) {
			debug("initialTagText: {0}, tagText: {1}", initialTagText, tagText);
		}

		return tagText;
	}

	private Tag parseTag(String text, int i) {
		Tag tag = new Tag();

		List<Integer> tagStart = new ArrayList<>();

		// int j = text.indexOf(">", i);
		int j = searchEnd(text, i, tagStart);

		tag.endPos = j;
		tag.startPos = i;

		// String tagText = text.substring(i + 1, j).trim();

		String tagText = getTagText(text, i, j, tagStart);

		if (tagText.startsWith("/")) {
			tag.close = true;
			tagText = tagText.substring(1);
		}
		if (tagText.endsWith("/")) {
			tag.closed = true;
			tagText = tagText.substring(0, tagText.length() - 1).trim();
		}

		j = WhitespaceSearcher.search(tagText);
		if (j < 0) {
			tag.name = tagText;
			return tag;
		}

		tag.name = tagText.substring(0, j).trim();
		tagText = tagText.substring(j).trim();

		while (true) {
			j = tagText.indexOf("=");
			if (j < 0) {
				break;
			}
			String attrName = tagText.substring(0, j).trim();
			tagText = tagText.substring(j + 1).trim();
			String attrValue = "";
			if (tagText.startsWith("\"")) {
				j = tagText.indexOf("\"", 1);
				attrValue = tagText.substring(1, j);
				tagText = tagText.substring(j + 1).trim();
			} else if (tagText.startsWith("'")) {
				j = tagText.indexOf("'", 1);
				attrValue = tagText.substring(1, j);
				tagText = tagText.substring(j + 1).trim();
			} else {
				j = WhitespaceSearcher.search(tagText, 1);
				if (j < 0) {
					j = tagText.length();
				}
				attrValue = tagText.substring(1, j);
				tagText = tagText.substring(j).trim();
			}
			attrName = attrName.toLowerCase();
			tag.attrs.put(attrName, attrValue);
		}
		return tag;
	}

	protected boolean hasText(String text, int i, String value) {
		if (text.length() < (i + value.length())) {
			return false;
		}
		String subStr = text.substring(i, i + value.length());
		if (subStr.equalsIgnoreCase(value)) {
			return true;
		}
		return false;
	}
}
