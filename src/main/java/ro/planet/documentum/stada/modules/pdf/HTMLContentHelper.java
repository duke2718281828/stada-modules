package ro.planet.documentum.stada.modules.pdf;

import java.awt.Desktop;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.math.BigInteger;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBElement;

import org.docx4j.TraversalUtil;
import org.docx4j.XmlUtils;
import org.docx4j.finders.RangeFinder;
import org.docx4j.jaxb.Context;
import org.docx4j.openpackaging.packages.WordprocessingMLPackage;
import org.docx4j.openpackaging.parts.WordprocessingML.MainDocumentPart;
import org.docx4j.wml.CTBookmark;
import org.docx4j.wml.CTMarkupRange;
import org.docx4j.wml.ContentAccessor;
import org.docx4j.wml.P;
import org.docx4j.wml.R;
import org.docx4j.wml.RPr;
import org.docx4j.wml.SdtBlock;
import org.docx4j.wml.SdtElement;
import org.docx4j.wml.SdtPr;
import org.docx4j.wml.Tag;
import org.docx4j.wml.Tbl;
import org.docx4j.wml.Text;
import org.jvnet.jaxb2_commons.ppp.Child;

/**
 * Вставляет в место закладки HTML файл.
 * 
 * Имя закладки, например, DMSxCP_Content.
 */
public class HTMLContentHelper extends DocxHelper {

    private P first = null;
    private P second = null;
    private P last = null;
    private int insertIndex;
    private boolean checkHasData = false;

    private String getProjectFolder() {
	String path = getClass().getResource(".").getPath();
	String mark = "stada-modules";
	int index = path.lastIndexOf(mark);
	path = path.substring(index + mark.length() + 1);
	return "";
    }

    public HTMLContentSource[] getTestContent() throws Exception {
	String inputfilepath = getProjectFolder() + "resources/Текст письма (8).html";
	// String inputfilepath1 = getProjectFolder() + "resources/Текст
	// письма.htm";
	String inputfilepath1 = getProjectFolder() + "resources/Текст письма (9).html";

	String[] paths = new String[] { inputfilepath, inputfilepath1 };
	String[] paths1 = new String[] { inputfilepath };
	HTMLContentSource[] result = new HTMLContentSource[paths.length];
	for (int i = 0; i < paths.length; i++) {
	    FileInputStream fis1 = new FileInputStream(paths[i]);
	    // Only for text, close file at program end.
	    result[i] = new HTMLContentSource(fis1);
	}
	return result;
    }

    /**
     * bookmarkStart bookmarkEnd
     * 
     * Пример:
     * 
     * <w:bookmarkStart w:id="0" w:name="DMSxCP_Content"/><w:bookmarkStart w:id=
     * "1" w:name="_GoBack"/><w:bookmarkEnd w:id="0"/><w:bookmarkEnd w:id="1"/>
     * 
     * Здесь w:bookmarkStart = CTBookmark, w:bookmarkEnd = CTMarkupRange
     * 
     * @throws Exception
     */
    protected String test() throws Exception {
	String contentStr1 = getProjectFolder() + "resources/528_EMA_EN_RU_3.docx";

	WordprocessingMLPackage wordMLPackage = WordprocessingMLPackage.load(new File(contentStr1));
	debug("Loaded {0}", contentStr1);

	HTMLContentSource[] source1 = getTestContent();
	HTMLContentSource[] source2 = getTestContent();

	try {

	    update(wordMLPackage, true, false, source1);
	    update(wordMLPackage, true, false, source2);

	} finally {
	    //
	}
	String outputName = "d:/output1.docx";
	wordMLPackage.save(new File(outputName));

	try {
	    Desktop.getDesktop().open(new File(outputName));
	} catch (Throwable tr) {
	    throw new RuntimeException(tr);
	}
	// this.traversalSearch(template.getMainDocumentPart(),)

	new HTMLContentExtractor().test(outputName);

	return outputName;
    }

    public void update(WordprocessingMLPackage wordMLPackage, boolean proceedBookmarks, boolean proceedForms,
	    InputStream in) throws Exception {
	update(wordMLPackage, proceedBookmarks, proceedForms, new HTMLContentSource(in));
    }

    public void update(WordprocessingMLPackage wordMLPackage, boolean proceedBookmarks, boolean proceedForms,
	    HTMLContentSource... source) throws Exception {
	for (HTMLContentSource item : source) {
	    item.initContent(wordMLPackage);
	}
	try {
	    if (proceedBookmarks) {
		updateBmk(wordMLPackage, source);
	    }
	} catch (Exception ex) {
	    error("Bookmark update error", ex);
	}
	try {
	    if (proceedForms) {
		updateSdt(wordMLPackage, source);
	    }
	} catch (Exception ex) {
	    error("Forms update error", ex);
	}
    }

    private void updateBmk(WordprocessingMLPackage wordMLPackage, HTMLContentSource... source) throws Exception {
	MainDocumentPart documentPart = wordMLPackage.getMainDocumentPart();

	RangeFinder rt = new RangeFinder("CTBookmark", "CTMarkupRange");
	new TraversalUtil(documentPart, rt);

	HTMLBookmarkParser bookmarkParser = new HTMLBookmarkParser();
	int bookmarkMaxIndex = bookmarkParser.getBookmarkMaxIndex(rt);

	debug("max bookmark index {0}", bookmarkMaxIndex);
	// debug("{0}", marshaltoDebugString(newContent));

	for (CTBookmark bm : rt.getStarts()) {

	    String name = bm.getName();
	    // do we have data for this one?
	    if (name == null) {
		continue;
	    }

	    debug("bookmark found {0}, {1}", name, bm.getId());

	    if (name.startsWith("DMSxCP_Content") == false) {
		continue;
	    }

	    List<Object> content = new HTMLContentSource().get(name, bookmarkMaxIndex, source);
	    if (content == null) {
		debug("skip {0}, no content", name);
		continue;
	    }
	    if (content.size() == 0) {
		debug("skip {0}, zero content", name);
		continue;
	    }

	    debug("bookmark process {0}, {1}", name, bm.getId());

	    CTMarkupRange bookMarkEnd = searchBookmarkEnd(rt, bm.getId());

	    removeBetween(bm, bookMarkEnd);

	    Object parent = bm.getParent();
	    debug("Parent {0}", parent.getClass());

	    P current = (P) parent;
	    Object grandParent = current.getParent();
	    debug("grandParent class: {0}", grandParent.getClass().getName());
	    ContentAccessor grand = (ContentAccessor) grandParent;

	    insertIndex = find(current, grand);

	    int j = insertIndex;

	    processNewContent(grand, content);

	    boolean useCurrentP = true;

	    if (useCurrentP) {
		current.getContent().clear();
		for (Object item : first.getContent()) {
		    current.getContent().add(XmlUtils.deepCopy(item));

		    // debug("{0}",
		    // marshaltoDebugString(XmlUtils.deepCopy(item)));
		}

		grand.getContent().remove(first);
		if (second == first) {
		    second = current;
		}
		first = current;
	    }

	    moveTo(bm, first, 0);
	    moveTo(bookMarkEnd, second, -1);

	    if (useCurrentP == false) {
		grand.getContent().remove(j);
	    }
	}
    }

    private void updateSdt(WordprocessingMLPackage wordMLPackage, HTMLContentSource... source) throws Exception {
	MainDocumentPart documentPart = wordMLPackage.getMainDocumentPart();

	final List<Couple<SdtElement, String>> list = new ArrayList<>();

	new TraversalUtil(documentPart, new TraversalUtil.CallbackImpl() {

	    @Override
	    public List<Object> apply(Object obj) {
		if (obj instanceof SdtElement) {
		    SdtElement element = (SdtElement) obj;
		    SdtPr sdtPr = element.getSdtPr();
		    String tagStr = "";
		    if (sdtPr != null) {
			for (Object item : sdtPr.getRPrOrAliasOrLock()) {
			    debug("Sdt item {0}", item.getClass().getName());
			    if (item instanceof Tag) {
				Tag tag = (Tag) item;
				tagStr = tag.getVal();
			    }
			}
		    }
		    // SdtBlock
		    debug("SdtElement {0}, tag {1}", element.getClass().getName(), tagStr);
		    if (tagStr.startsWith("DMSxCP_Content")) {
			if (element instanceof SdtBlock) {
			    list.add(new Couple<>(element, tagStr));
			}
		    }
		}
		return null;
	    }
	});

	HTMLBookmarkParser bookmarkParser = new HTMLBookmarkParser();
	int bookmarkMaxIndex = bookmarkParser.getBookmarkMaxIndex(list);

	debug("Max bookmark index {0}", bookmarkMaxIndex);

	for (Couple<SdtElement, String> item : list) {
	    String name = item.getSecond();

	    List<Object> content = new HTMLContentSource().get(name, bookmarkMaxIndex, source);
	    if (content == null) {
		debug("Skip {0}, no content", name);
		continue;
	    }
	    if (content.size() == 0) {
		debug("Skip {0}, zero content", name);
		continue;
	    }

	    debug("Sdt element process {0}", name);

	    SdtBlock block = (SdtBlock) item.getFirst();
	    ContentAccessor contentAccessor = block.getSdtContent();
	    contentAccessor.getContent().clear();
	    contentAccessor.getContent().addAll(content);
	}
    }

    private void addChild(ContentAccessor grand, Child child) {
	grand.getContent().add(insertIndex + 1, child);
	insertIndex++;
    }

    private void removeBetween(CTBookmark bm, CTMarkupRange bookMarkEnd) throws Exception {
	Object parent = bm.getParent();
	Child current = (Child) parent;

	Object grandParent = current.getParent();
	ContentAccessor grand = (ContentAccessor) grandParent;

	while (true) {
	    int index;

	    if (current instanceof P) {
		P currentP = (P) current;
		if (current == bm.getParent()) {
		    index = find(bm, currentP) + 1;
		} else {
		    index = 0;
		}

		while (index < currentP.getContent().size()) {
		    Object item = unwrap(currentP.getContent().get(index));
		    if (item == bookMarkEnd) {
			return;
		    }
		    currentP.getContent().remove(index);
		}
	    } else {
		debug("Remove item{0}", current.getClass().getName());
		// grand.getContent().remove(current);
	    }

	    index = find(current, grand);
	    index++;
	    if (index >= grand.getContent().size()) {
		return;
	    }

	    Child newCurrent = (Child) grand.getContent().get(index);
	    if (current instanceof Tbl) {
		grand.getContent().remove(current);
	    }
	    if (current instanceof P) {
		P currentP = (P) current;
		if (currentP.getContent().size() == 0) {
		    grand.getContent().remove(current);
		}
	    }
	    current = newCurrent;
	}
    }

    protected boolean hasData(Object item) {
	if (item instanceof R) {
	    R r = (R) item;
	    for (Object ch : r.getContent()) {
		if (hasData(ch)) {
		    debug("Has data/r {0}", ch.getClass().getName());
		    return true;
		}
	    }
	    return false;
	} else if (item instanceof Text) {
	    Text text = (Text) item;
	    debug("Has data/text {0}, {1}", text.getValue(), (int) text.getValue().charAt(0));
	    return text.getValue().trim().length() > 0;
	} else if (item instanceof RPr) {
	    return false;
	} else

	if (item instanceof P) {
	    P p = (P) item;
	    for (Object ch : p.getContent()) {
		if (hasData(ch)) {
		    debug("Has data/p {0}", ch.getClass().getName());
		    return true;
		}
	    }
	    return false;
	} else {
	    debug("Has data/default -> {0}", item.getClass().getName());
	    return true;
	}
    }

    private P processNewP(ContentAccessor grand, P p) {
	if (p == null) {
	    p = Context.getWmlObjectFactory().createP();
	}
	addChild(grand, p);
	if (first == null) {
	    first = p;
	}
	second = p;
	return p;
    }

    private void processNewContent(ContentAccessor grand, List<Object> newContent) throws Exception {
	first = null;
	second = null;
	last = null;
	Object lastItem = null;

	for (Object item : newContent) {

	    debug("hasData: {0}, marshaltoDebugString: {1}", hasData(item), marshaltoDebugString(item));

	    if (checkHasData) {
		if (hasData(item) == false) {
		    continue;
		}
	    }

	    debug("Add new {0}", item.getClass().getName());
	    if (item instanceof P) {

		processNewP(grand, (P) item);

		last = null;
	    } else if (item instanceof Tbl) {
		if (first == null) {
		    /*
		     * Добавляем один абзац в начало. Если таблица в начале.
		     * Чтобы поместить тег начала закладки в него.
		     */
		    processNewP(grand, null);
		}

		addChild(grand, (Tbl) item);

		lastItem = item;

		last = null;
	    } else {
		if (last == null) {
		    last = processNewP(grand, null);
		}
		last.getContent().add(item);
		// throw new Exception("Unsupported HTML content");
	    }

	}

	/*
	 * Добавляем еще один абзац в конец. Если таблица в конце. Чтобы
	 * поместить тег окончания закладки в него.
	 */
	if (lastItem instanceof Tbl) {
	    processNewP(grand, null);
	}
    }

    private void moveTo(CTMarkupRange item, ContentAccessor target, int index) throws Exception {
	Object parent = item.getParent();
	if (parent instanceof ContentAccessor) {
	    ContentAccessor cs = (ContentAccessor) parent;
	    cs.getContent().remove(item);
	    if (index < 0) {
		target.getContent().add(item);
	    } else {
		target.getContent().add(index, item);
	    }
	    item.setParent(target);

	} else {
	    throw new Exception("Unsupported parent: " + parent.getClass().getName());
	}

    }

    private CTMarkupRange searchBookmarkEnd(RangeFinder rt, BigInteger id) throws Exception {

	for (CTMarkupRange bm : rt.getEnds()) {
	    if (bm.getId().equals(id)) {
		return bm;
	    }
	}

	throw new Exception(MessageFormat.format("Cannot obtain bookmark end {0}", id));
    }

    private int find(Object c, ContentAccessor current) throws Exception {
	List<Object> children = current.getContent();
	for (int i = 0; i < children.size(); i++) {
	    Object item = unwrap(children.get(i));

	    if (item == c) {
		return i;
	    }
	    debug("{0}", item.getClass().getName());
	}
	throw new Exception("Bookmark not found");
    }

    @SuppressWarnings("rawtypes")
    protected int find(String name, P current) throws Exception {
	debug("Start search {0}", name);
	List<Object> children = current.getContent();
	int result = -1;
	for (int i = 0; i < children.size(); i++) {
	    Object item = children.get(i);
	    if (item instanceof JAXBElement) {
		JAXBElement el = (JAXBElement) item;
		item = el.getValue();
	    }
	    if (item instanceof CTBookmark) {
		CTBookmark bookmark = (CTBookmark) item;
		if (name.equals(bookmark.getName())) {
		    debug("Found {0}", i);
		    result = i;
		}
	    }

	    if (item instanceof CTBookmark) {
		CTBookmark bookmark = (CTBookmark) item;
		debug("Bookmark {0}, {1}", bookmark.getId(), bookmark.getName());
	    } else if (item instanceof CTMarkupRange) {
		CTMarkupRange bookmark = (CTMarkupRange) item;
		debug("Markup range {0}", bookmark.getId());
	    } else {
		debug("{0}", item.getClass().getName());
	    }
	}
	if (result < 0) {
	    throw new Exception("Bookmark not found");
	}
	return result;
    }

    public static void main(String[] args) {

	try {
	    new HTMLContentHelper().test();
	} catch (Throwable tr) {
	    System.out.println(tr.getClass().getName());
	    System.out.println(tr.getMessage());
	    tr.printStackTrace();
	}
    }
}
