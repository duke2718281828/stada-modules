package ro.planet.documentum.stada.modules.pdf;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.docx4j.openpackaging.packages.WordprocessingMLPackage;

import com.documentum.fc.client.IDfFolder;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.client.IDfUser;
import com.documentum.fc.common.DfId;

import framework.ru.documentum.utils.CreateFolderStructureHelper;
import framework.ru.documentum.utils.SessionHelper;
import framework.ru.documentum.utils.StringUtils;

/**
 * Добавлен метод extractHtml в TemplatePopulateModule, берет на вход ID
 * документа docx, создает временные объекты, каждый и которых содержит HTML
 * контент из Form-поля вида Rich Text. Имя объекта соответствует тегу
 * Form-поля. ID объектов, через запятую, возвращаюся. Объекты кладуться в папку
 * Temp внутри личной папки пользователя.
 * 
 * ID файлов отсортированы в соответствии с именем. Обрабатываемые поля должны
 * иметь тег DMSxCP_Content*, где * - номер или пусто (что соответствует 0). В
 * соответствии с этим номером идет сортировка по возрастанию.
 * 
 *
 */
public class HTMLExtractHelper extends DocxHelper {

    private IDfSession session;
    private String targetFolderId;

    private HTMLExtractHelper() {

    }

    public HTMLExtractHelper(IDfSession session) throws Exception {
	this.session = session;

	initTargetFolder();
    }

    private void initTargetFolder() throws Exception {
	IDfUser user = session.getUser(null);
	String defaultFolder = user.getDefaultFolder();
	debug("User {0}, default folder {1}", user.getUserName(), defaultFolder);

	IDfFolder folder = session.getFolderBySpecification(defaultFolder);

	String targetFolderPath = folder.getFolderPath(0) + "/Temp";
	CreateFolderStructureHelper helper = new CreateFolderStructureHelper(session);
	helper.setOwnerName(folder.getOwnerName());
	IDfFolder targetFolder = helper.queryFolder(targetFolderPath);
	debug("Target folder {0}", targetFolder.getFolderPath(0));
	targetFolderId = targetFolder.getObjectId() + "";
    }

    private class Item {
	private ByteArrayOutputStream str = new ByteArrayOutputStream();
	private String name;
	private File file;
	private String fileName;

	@Override
	public String toString() {
	    return name + "/" + str.toByteArray().length;
	}
    }

    private Map<String, Item> map = new LinkedHashMap<>();

    public String execute(String objectId) throws Exception {
	debug("extract html started");
	debug("objectId: {0}", objectId);
	IDfSysObject templateObject = (IDfSysObject) session.getObject(new DfId(objectId));
	boolean isHTML = false;
	String contentType = templateObject.getContentType();
	debug("contentType: {0}", contentType);
	if (contentType != null && contentType.toLowerCase().startsWith("htm")) {
	    isHTML = true;
	}
	List<String> ids = new ArrayList<>();
	List<Item> list = new ArrayList<>();
	debug("isHTML: {0}", isHTML);
	if (isHTML == true) {
	    String fileName = templateObject.getFile("html_content");
	    debug("templateObject temp file: {0}", fileName);
	    File htmlFile = new File(fileName);
	    Item item = new Item();
	    item.name = "html_content";
	    item.fileName = fileName;
	    item.file = htmlFile;
	    list.add(item);
	} else {
	    InputStream content = templateObject.getContent();
	    debug("content size: {0}", content.available());
	    WordprocessingMLPackage template = WordprocessingMLPackage.load(content);
	    HTMLContentExtractor extractor = new HTMLContentExtractor();
	    extractor.extract(template, new IHTMLContentExtractorProcessor() {
		@Override
		public OutputStream newFile(String name) throws Exception {
		    Item item = new Item();
		    item.name = name;
		    map.put(name, item);
		    return item.str;
		}
	    });
	    list = new ArrayList<>(map.values());
	    Collections.sort(list, new Comparator<Item>() {
		@Override
		public int compare(Item arg0, Item arg1) {
		    HTMLBookmarkParser parser = new HTMLBookmarkParser();
		    return parser.parseIndex(arg0.name) - parser.parseIndex(arg1.name);
		}
	    });
	    debug("sorting has been completed {0}", Arrays.toString(list.toArray()));
	}
	for (Item item : list) {
	    IDfSysObject sys = (IDfSysObject) session.newObject("dm_document");
	    sys.setObjectName(item.name);
	    sys.setContentType("html");
	    if (item.file != null) {
		sys.setFile(item.fileName);
	    } else {
		sys.setContent(item.str);
	    }
	    sys.link(targetFolderId);
	    sys.setLogEntry("html/temp/extract/docx");
	    sys.save();
	    if (item.file != null) {
		item.file.delete();
	    }
	    debug("created document: {0}", sys.getObjectId().getId());
	    ids.add(sys.getObjectId().toString());
	}
	debug("Result {0}", Arrays.toString(ids.toArray()));
	return StringUtils.joinList(ids, ",");
    }

    private void test() throws Exception {
	HTMLContentHelper helper = new HTMLContentHelper();
	String fileName = helper.test();

	IDfSysObject sys = (IDfSysObject) session.newObject("dm_document");
	sys.setObjectName("Test1.htm");
	sys.setContentType("msw12");
	sys.setFile(fileName);
	sys.link(session.getUser(null).getDefaultFolder());
	sys.setLogEntry("html/temp/extract/docx/test");
	sys.save();

	String result = execute(sys.getObjectId().toString());

	debug("Test result {0}", result);

	for (String item : result.split(",")) {
	    sys = (IDfSysObject) session.getObject(new DfId(item));
	    sys.getFile("E:/TestExtract" + item + ".htm");
	}
    }

    private void testHtml() throws Exception {
	IDfSysObject sys = (IDfSysObject) session.newObject("dm_document");
	sys.setObjectName("test_html_1.htm");
	sys.setContentType("html");
	sys.setFile("d:/html_orig.htm");
	sys.link(session.getUser(null).getDefaultFolder());
	sys.setLogEntry("html/temp/extract/docx/test");
	sys.save();
	String result = execute(sys.getObjectId().toString());
	debug("test result {0}", result);
	for (String item : result.split(",")) {
	    sys = (IDfSysObject) session.getObject(new DfId(item));
	    sys.getFile("d:/test_html_1_extracted" + item + ".htm");
	}
    }

    public static void main(String[] args) {
	String docbase = args[0];
	String userName = args[1];
	String ticket = args[2];
	try {
	    SessionHelper sessionHelper = new SessionHelper((IDfSession) null);
	    IDfSession session = sessionHelper.getUserSession(userName, ticket, docbase);
	    try {
		HTMLExtractHelper helper = new HTMLExtractHelper(session);
		// helper.test();
		helper.testHtml();
	    } finally {
		sessionHelper.release();
	    }
	} catch (Throwable tr) {
	    System.out.println(tr.getClass().getName());
	    System.out.println(tr.getMessage());
	    tr.printStackTrace();
	    new HTMLExtractHelper().error("Error", tr);
	}
    }
}
