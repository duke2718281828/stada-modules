package ro.planet.documentum.stada.modules.pdf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.docx4j.convert.in.xhtml.FormattingOption;
import org.docx4j.convert.in.xhtml.XHTMLImporterImpl;
import org.docx4j.openpackaging.packages.WordprocessingMLPackage;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.w3c.tidy.Tidy;

import com.documentum.fc.client.IDfSession;

import framework.ru.documentum.services.DsHelper;
import framework.ru.documentum.utils.IOHelper;
import framework.ru.documentum.utils.StringUtils;

public class HTMLContentSource extends DsHelper {

	public HTMLContentSource() {
		super((IDfSession) null);
	}

	public HTMLContentSource(InputStream stream) {
		super((IDfSession) null);
		this.stream = stream;
	}

	private InputStream stream;

	private List<Object> newContent;

	public InputStream getStream() {
		return stream;
	}

	public void setStream(InputStream stream) {
		this.stream = stream;
	}

	public String getText1() throws Exception {
		Document document = Jsoup.parse(stream, null, "");
		document.outputSettings().syntax(Document.OutputSettings.Syntax.xml);
		String stringFromFile = document.html();
		// String stringFromFile = StringUtils.readToString(stream, "UTF-8");
		return prepareHTML(stringFromFile);
	}

	public String getText2() throws Exception {
		ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
		new IOHelper().copy(stream, byteArray);
		String stringFromFile = "";
		try {
			stringFromFile = new String(byteArray.toByteArray(), "UTF-8");
			debug("content: {0}", stringFromFile);
		} catch (Throwable tr) {
			debug("non unicode content");
		}

		boolean canFixBody = true;

		if (canFixBody) {
			stringFromFile = new HTMLStyleConvertor().execute(stringFromFile);

			debug("pre fix: {0}", stringFromFile);

			if (stringFromFile.indexOf("<body") < 0 && stringFromFile.length() > 0) {
				/*
				 * Tidy не работает, если нет body. Возвращает пустой текст.
				 * Либо исправляем, либо не запускаем tidy.
				 */
				stringFromFile = "<body>" + stringFromFile + "</body>";
			}

			byteArray = new ByteArrayOutputStream();
			byteArray.write(stringFromFile.getBytes("UTF-8"));
		}

		boolean useJsoup = true;
		byte[] bytes;

		if (stringFromFile.toLowerCase().indexOf("<body") > -1 && useJsoup == false) {
			ByteArrayInputStream inp1 = new ByteArrayInputStream(byteArray.toByteArray());

			ByteArrayOutputStream str = new ByteArrayOutputStream();
			Tidy tidy = new Tidy();
			tidy.setXHTML(true);
			// tidy.setWraplen(10);
			tidy.setXmlTags(true);
			tidy.setInputEncoding("UTF-8");
			tidy.setOutputEncoding("UTF-8");
			tidy.setShowErrors(1);
			tidy.parse(inp1, str);
			int errors = tidy.getParseErrors();

			bytes = str.toByteArray();

			String stringFromFile1 = StringUtils.readToString(new ByteArrayInputStream(bytes), "UTF-8");
			stringFromFile1 = stringFromFile1.trim();

			debug("tidy, errors: {0}, bytes: {1}, string text: {2}", errors, bytes.length, stringFromFile1.length());

			if (stringFromFile1.length() == 0) {
				bytes = new byte[] {};
			}
		} else {
			debug("skip tidy");
			bytes = new byte[] {};
		}

		if (bytes.length == 0) {
			debug("use JSoup");
			ByteArrayInputStream inp1 = new ByteArrayInputStream(byteArray.toByteArray());
			Document document = Jsoup.parse(inp1, null, "");
			debug("JSoup encoding: {0}", document.charset());
			document.outputSettings().syntax(Document.OutputSettings.Syntax.xml);
			stringFromFile = document.html();
			bytes = stringFromFile.getBytes("UTF-8");
		}

		ByteArrayInputStream input = new ByteArrayInputStream(bytes);
		stringFromFile = StringUtils.readToString(input, "UTF-8");
		return prepareHTML(stringFromFile);
	}

	public String getText3() throws Exception {
		String stringFromFile = StringUtils.readToString(stream, "UTF-8");
		return prepareHTML(stringFromFile);
	}

	public String getText() throws Exception {
		return getText2();
	}

	public void initContent(WordprocessingMLPackage wordMLPackage) throws Exception {
		if (stream == null) {
			return;
		}

		// Convert the XHTML, and add it into the empty docx we made
		XHTMLImporterImpl XHTMLImporter = new XHTMLImporterImpl(wordMLPackage);

		String stringFromFile = getText();

		String unescaped = stringFromFile;
		String baseURL = "";
		XHTMLImporter.setHyperlinkStyle("Hyperlink");

		// XHTMLImporter.setParagraphFormatting(FormattingOption.CLASS_PLUS_OTHER);
		// XHTMLImporter.setRunFormatting(FormattingOption.CLASS_PLUS_OTHER);

		newContent = XHTMLImporter.convert(unescaped, baseURL);
	}

	private String preprocessHTML(String stringFromFile) {
		if (stringFromFile.startsWith("\uFEFF")) {
			stringFromFile = stringFromFile.substring(1);
		}
		stringFromFile = stripXMLDeclaration(stringFromFile);
		String xmlPrefix = "<html xmlns:v='urn:schemas-microsoft-com:vml' xmlns:o='urn:schemas-microsoft-com:office:office' xmlns:w='urn:schemas-microsoft-com:office:word' xmlns:m='http://schemas.microsoft.com/office/2004/12/omml'>";
		if (stringFromFile.indexOf("<body") < 0) {
			stringFromFile = "<body>" + stringFromFile + "</body>";
		}
		stringFromFile = xmlPrefix + stringFromFile + "</html>";
		return stringFromFile;
	}

	private String stripXMLDeclaration(String value) {
		int i = value.indexOf("<?xml");
		if (i > -1) {
			int j = value.indexOf("?>", i);
			if (j > -1) {
				return value.substring(0, i) + value.substring(j + 2);
			}
		}
		return value;
	}

	private String prepareHTML(String stringFromFile) throws Exception {
		stringFromFile = preprocessHTML(stringFromFile);
		stringFromFile = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" + stringFromFile;
		debug("initial html: {0}", stringFromFile);
		stringFromFile = new HTMLStyleConvertor().execute(stringFromFile);
		debug("final html: {0}", stringFromFile);
		return stringFromFile;
	}

	public List<Object> getNewContent() {
		return newContent;
	}

	public List<Object> get(String name, int maxIndex, HTMLContentSource... list) throws Exception {

		int index = new HTMLBookmarkParser().parseIndex(name);
		boolean last = index == maxIndex;

		if (last) {
			List<Object> result = new ArrayList<>();
			debug("get content from {0} to {1}", index, list.length - 1);

			for (int i = index; i < list.length; i++) {
				HTMLContentSource html = list[i];
				if (html.getNewContent() == null) {
					debug("skip index {0}, name {1}, max {2}", i, name, maxIndex);
				} else {
					result.addAll(html.getNewContent());
				}
			}

			return result;
		} else {
			if (index >= list.length) {
				debug("unsupported index {0}, {1}, {2}", name, index, list.length);
				return null;
			}
			HTMLContentSource html = list[index];
			if (html.getNewContent() == null) {
				debug("skip name {0}, max {1}", name, maxIndex);
				return null;
			}
			return html.getNewContent();
		}

	}

	private String getProjectFolder() {
		String path = getClass().getResource(".").getPath();
		String mark = "stada-modules";
		int index = path.lastIndexOf(mark);
		path = path.substring(index + mark.length() + 1);
		return "";
	}

	private void test() throws Exception {
		String inputfilepath1 = getProjectFolder() + "resources/Текст письма 5.htm";
		FileInputStream str = new FileInputStream(inputfilepath1);
		try {

			HTMLContentSource item = new HTMLContentSource(str);
			String text = item.getText2();
		} finally {
			str.close();
		}
	}

	public static void main(String[] args) {

		try {
			new HTMLContentSource().test();
		} catch (Throwable tr) {
			System.out.println(tr.getClass().getName());
			System.out.println(tr.getMessage());
			tr.printStackTrace();
		}
	}
}
