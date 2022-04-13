package ro.planet.documentum.stada.modules.pdf;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.docx4j.Docx4J;
import org.docx4j.TraversalUtil;
import org.docx4j.XmlUtils;
import org.docx4j.convert.out.HTMLSettings;
import org.docx4j.openpackaging.packages.WordprocessingMLPackage;
import org.docx4j.openpackaging.parts.WordprocessingML.MainDocumentPart;
import org.docx4j.wml.ContentAccessor;
import org.docx4j.wml.P;
import org.docx4j.wml.R;
import org.docx4j.wml.SdtBlock;
import org.docx4j.wml.SdtElement;
import org.docx4j.wml.SdtPr;
import org.docx4j.wml.Tag;
import org.docx4j.wml.Text;

public class HTMLContentExtractor extends DocxHelper {

    private IHTMLContentExtractorProcessor processor;

    public void extract(WordprocessingMLPackage wordMLPackage, IHTMLContentExtractorProcessor processor)
	    throws Exception {
	debug("Extract html from sdt elements");

	this.processor = processor;

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

	for (Couple<SdtElement, String> item : list) {
	    extract(item);
	}
    }

    private void removeEmpty(ContentAccessor contentAccessor) throws Exception {
	List<Object> remove = new ArrayList<>();
	for (Object child : contentAccessor.getContent()) {
	    Object line = unwrap(child);

	    // debug("Check item {0}", line.getClass().getName());

	    if (line instanceof Text) {
		Text text = (Text) line;
		// debug("Text {0}", text.getValue());

		if (text.getValue().trim().length() == 0) {
		    remove.add(child);
		}
	    }
	    if (line instanceof R) {
		R text = (R) line;
		removeEmpty(text);
		if (text.getContent().size() == 0) {
		    remove.add(child);
		}
	    }
	}
	contentAccessor.getContent().removeAll(remove);
    }

    private void extract(Couple<SdtElement, String> item) throws Exception {
	debug("Extract html from sdt element {0}", item.getSecond());

	WordprocessingMLPackage wordMLPackage = WordprocessingMLPackage.createPackage();
	SdtElement element = item.getFirst();
	ContentAccessor contentAccessor = element.getSdtContent();
	debug("Content size {0}", contentAccessor.getContent().size());

	MainDocumentPart part = wordMLPackage.getMainDocumentPart();

	for (Object child : contentAccessor.getContent()) {
	    debug("Add content {0}", child.getClass().getName());
	    if (child instanceof P) {

		P childP = (P) child;

		removeEmpty(childP);

		if (childP.getContent().size() == 0) {
		    continue;
		}

	    }
	    debug("Check {0}", marshaltoDebugString(child));

	    part.getContent().add(XmlUtils.deepCopy(child));
	}

	HTMLSettings htmlSettings = Docx4J.createHTMLSettings();
	htmlSettings.setWmlPackage(wordMLPackage);
	htmlSettings.setImageDirPath("");
	htmlSettings.setImageTargetUri("docximage");
	htmlSettings.setImageIncludeUUID(true);
	// htmlSettings.setUserBodyTop("<STYLE>td{vertical-align:top}</STYLE>");

	OutputStream out = processor.newFile(item.getSecond());

	Docx4J.toHTML(htmlSettings, out, Docx4J.FLAG_NONE);
    }

    public void test(String fileName) throws Exception {
	WordprocessingMLPackage wordMLPackage = WordprocessingMLPackage.load(new File(fileName));
	extract(wordMLPackage, new IHTMLContentExtractorProcessor() {

	    @Override
	    public OutputStream newFile(String name) throws Exception {
		return new FileOutputStream("E:/Temp/" + name + ".htm");
	    }
	});
    }
}
