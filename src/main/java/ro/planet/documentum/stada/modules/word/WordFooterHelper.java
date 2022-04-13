package ro.planet.documentum.stada.modules.word;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.docx4j.XmlUtils;
import org.docx4j.dml.wordprocessingDrawing.Inline;
import org.docx4j.jaxb.Context;
import org.docx4j.model.structure.HeaderFooterPolicy;
import org.docx4j.model.structure.SectionWrapper;
import org.docx4j.openpackaging.exceptions.InvalidFormatException;
import org.docx4j.openpackaging.packages.WordprocessingMLPackage;
import org.docx4j.openpackaging.parts.Part;
import org.docx4j.openpackaging.parts.PartName;
import org.docx4j.openpackaging.parts.WordprocessingML.BinaryPartAbstractImage;
import org.docx4j.openpackaging.parts.WordprocessingML.FooterPart;
import org.docx4j.openpackaging.parts.WordprocessingML.HeaderPart;
import org.docx4j.openpackaging.parts.relationships.RelationshipsPart;
import org.docx4j.openpackaging.parts.relationships.RelationshipsPart.AddPartBehaviour;
import org.docx4j.relationships.Relationship;
import org.docx4j.relationships.Relationships;
import org.docx4j.wml.BooleanDefaultTrue;
import org.docx4j.wml.Br;
import org.docx4j.wml.CTRel;
import org.docx4j.wml.Drawing;
import org.docx4j.wml.FooterReference;
import org.docx4j.wml.Hdr;
import org.docx4j.wml.HeaderReference;
import org.docx4j.wml.Ftr;
import org.docx4j.wml.HdrFtrRef;
import org.docx4j.wml.Jc;
import org.docx4j.wml.JcEnumeration;
import org.docx4j.wml.ObjectFactory;
import org.docx4j.wml.P;
import org.docx4j.wml.PPr;
import org.docx4j.wml.PPrBase.TextAlignment;
import org.docx4j.wml.R;
import org.docx4j.wml.STBrType;
import org.docx4j.wml.SectPr;
import org.docx4j.wml.Text;

public class WordFooterHelper {

    private WordprocessingMLPackage template;
    private static ObjectFactory factory;
    private List<HeaderPart> headers = new ArrayList<HeaderPart>();
    private List<FooterPart> footers = new ArrayList<FooterPart>();
    private Map<String, HeaderPart> headersMap = new HashMap<String, HeaderPart>();
    private Map<String, FooterPart> footersMap = new HashMap<String, FooterPart>();
    private Map<String, String> headersRefMap = new HashMap<String, String>();
    private Map<String, String> footersRefMap = new HashMap<String, String>();
    private String footerFirstPageName = "/word/first-page-footer.xml";
    private String footerDefaultPageName = "/word/content-header.xml";
    private String footerEventPageName = "/word/content-even-header.xml";
    private int targetSection = -1;
    private HeaderFooterPolicy targetHeaderFooterPolicy;
    private Map<String, HeaderPart> headersNameMap = new HashMap<String, HeaderPart>();
    private Map<String, FooterPart> footersNameMap = new HashMap<String, FooterPart>();

    private boolean hasDefaultFooter() {
	return footersNameMap.containsKey(footerFirstPageName) && footersNameMap.containsKey(footerDefaultPageName);
    }

    public boolean hasDefaultFooter(WordprocessingMLPackage template) throws Exception {
	this.template = template;
	listDocument(template);
	return hasDefaultFooter();
    }

    public void addImageOnFirstPage(InputStream source, String firstText, String defaultText, byte[] image,
	    String target) throws Exception {
	template = WordprocessingMLPackage.load(source);
	addImageOnFirstPage(template, firstText, defaultText, image);

	// template.setTitle("Test");
	template.save(new File(target));
    }

  public void addImageOnFirstPage(WordprocessingMLPackage template, String firstText, String defaultText, byte[] image)
	    throws Exception {
	this.template = template;
	factory = Context.getWmlObjectFactory();
	Relationship relationship;

	targetSection = 0;

	listDocument(template);

	/**
	 * Есть только DEFAULT колонтитул. Жалуются, что колонтитул исчезает,
	 * полностью заменяется на новый. Поэтому копируем его как FIRST, если
	 * FIRST нет.
	 */
	if ((targetHeaderFooterPolicy.getFirstFooter() == null)
		&& (targetHeaderFooterPolicy.getDefaultFooter() != null)) {
	    int i = 0;
	    for (String rel : footersMap.keySet()) {
		FooterPart part = footersMap.get(rel);
		if ("DEFAULT".equals(footersRefMap.get(rel))) {
		    debug("Insert footer copy {0}", rel);
		    relationship = createFooterPart("/word/first-page-footer-source" + (i++) + ".xml", part);
		    createFooterReference(relationship, HdrFtrRef.FIRST);
		}
	    }
	}

	if ((targetHeaderFooterPolicy.getFirstHeader() == null)
		&& (targetHeaderFooterPolicy.getDefaultHeader() != null)) {
	    int i = 0;
	    for (String rel : headersMap.keySet()) {
		HeaderPart part = headersMap.get(rel);
		if ("DEFAULT".equals(headersRefMap.get(rel))) {
		    debug("Insert header copy {0}", rel);
		    relationship = createHeaderPart("/word/first-page-header-source" + (i++) + ".xml", part);
		    createHeaderReference(relationship, HdrFtrRef.FIRST);
		}
	    }
	}

	// Update if exists
	if (hasDefaultFooter()) {
	    debug("Update footers");
	    updateFooterPart(footersNameMap.get(footerFirstPageName), firstText, image);
	    updateFooterPart(footersNameMap.get(footerDefaultPageName), defaultText, null);
	} else {
	    debug("Create footers");

	    relationship = createFooterPart(footerFirstPageName, firstText, image);
	    createFooterReference(relationship, HdrFtrRef.FIRST);

	    relationship = createFooterPart(footerDefaultPageName, defaultText, null);
	    createFooterReference(relationship, HdrFtrRef.DEFAULT);

	    boolean checkEven = false;
	    if (checkEven) {
		if (headersRefMap.containsKey("EVEN")) {
		    relationship = createFooterPart(footerEventPageName, defaultText, null);
		    createFooterReference(relationship, HdrFtrRef.EVEN);
		}
	    }
	}

	// template.getMainDocumentPart().addObject(makePageBr());
    }

    public void listDocument(String path) throws Exception {
	debug("List document {0}", path);

	InputStream content = new FileInputStream(path);
	template = WordprocessingMLPackage.load(content);
	listDocument(template);
    }

    public void listDocument(WordprocessingMLPackage template) throws Exception {
	List<SectionWrapper> sections = template.getDocumentModel().getSections();
	for (int i = 0; i < sections.size(); i++) {

	    if (targetSection >= 0) {
		if (i != targetSection) {
		    continue;
		}
	    }
	    debug("Section {0}", i);
	    SectPr sectionProperties = sections.get(i).getSectPr();
	    debug("Has properties {0}", sectionProperties != null);

	    if (sectionProperties != null) {
		HeaderFooterPolicy policy = sections.get(i).getHeaderFooterPolicy();
		debug("Default footer {0}", policy.getDefaultFooter() != null);
		debug("Default header {0}", policy.getDefaultHeader() != null);
		debug("First footer {0}", policy.getFirstFooter() != null);
		debug("First header {0}", policy.getFirstHeader() != null);
		debug("Even footer {0}", policy.getEvenFooter() != null);
		debug("Even header {0}", policy.getEvenHeader() != null);

		debug("Header/Footer references {0}", sectionProperties.getEGHdrFtrReferences().size());

		targetHeaderFooterPolicy = policy;

		for (CTRel rel : sectionProperties.getEGHdrFtrReferences()) {
		    debug("{0}", rel.getId());
		    if (rel instanceof FooterReference) {
			debug("Footer reference");
			FooterReference ref = (FooterReference) rel;
			debug("Type {0}", ref.getType());
			footersRefMap.put(rel.getId(), "" + ref.getType());
		    }
		    if (rel instanceof HeaderReference) {
			debug("Header reference");
			HeaderReference ref = (HeaderReference) rel;
			debug("Type {0}", ref.getType());
			headersRefMap.put(rel.getId(), "" + ref.getType());
		    }
		}
	    }
	}
	RelationshipsPart relationshipPart = template.getMainDocumentPart().getRelationshipsPart();
	Relationships q = relationshipPart.getJaxbElement();
	List<Relationship> list = q.getRelationship();
	for (Relationship rels : list) {

	    if (targetSection >= 0) {
		if ((headersRefMap.containsKey(rels.getId()) || footersRefMap.containsKey(rels.getId())) == false) {
		    debug("Relation {0} skipped", rels.getId());
		    continue;
		}
	    }

	    // debug("Relationship {0}", rels.getType());
	    Part part = relationshipPart.getPart(rels);
	    // debug("Part class {0}", part == null ? "NULL" :
	    // part.getClass().getName());
	    if (part instanceof FooterPart) {
		debug("Footer found");
		FooterPart footer = (FooterPart) part;
		debug("Footer {0}, {1}, {2}", footer.getRelationshipType(), footer.getPartName(), rels.getId());
		footers.add(footer);
		footersMap.put(rels.getId(), footer);
		footersNameMap.put(footer.getPartName().getName(), footer);
	    }
	    if (part instanceof HeaderPart) {
		debug("Header found");
		HeaderPart header = (HeaderPart) part;
		debug("Header {0}, {1}, {2}", header.getRelationshipType(), header.getPartName(), rels.getId());
		headers.add(header);
		headersMap.put(rels.getId(), header);
		headersNameMap.put(header.getPartName().getName(), header);
	    }
	}
    }

    private static P makePageBr() throws Exception {
	P p = factory.createP();
	R r = factory.createR();
	Br br = factory.createBr();
	br.setType(STBrType.PAGE);
	r.getContent().add(br);
	p.getContent().add(r);
	return p;
    }

    private Relationship createFooterPart(String partName, String text, byte[] imageData) throws Exception {
	FooterPart footerPart = new FooterPart(new PartName(partName));
	footerPart.setPackage(template);
	debug("Part name {0}", footerPart.getPartName().getName());
	footerPart.setJaxbElement(createFooter(text, imageData, footerPart));
	Relationship result = template.getMainDocumentPart().addTargetPart(footerPart);
	return result;
    }

    private Relationship createFooterPart(String partName, FooterPart source) throws Exception {
	FooterPart footerPart = new FooterPart(new PartName(partName));
	footerPart.setPackage(template);
	debug("Part name {0}", footerPart.getPartName().getName());
	Ftr ftr = (Ftr) XmlUtils.deepCopy(source.getJaxbElement());
	footerPart.setJaxbElement(ftr);
	Relationship result = template.getMainDocumentPart().addTargetPart(footerPart);
	return result;
    }

    private Relationship createHeaderPart(String partName, HeaderPart source) throws Exception {
	HeaderPart part = new HeaderPart(new PartName(partName));
	part.setPackage(template);
	debug("Part name {0}", part.getPartName().getName());
	Hdr ftr = (Hdr) XmlUtils.deepCopy(source.getJaxbElement());
	part.setJaxbElement(ftr);
	Relationship result = template.getMainDocumentPart().addTargetPart(part);
	return result;
    }

    private void createFooterReference(Relationship relationship, HdrFtrRef type) {
	List<SectionWrapper> sections = template.getDocumentModel().getSections();

	SectPr sectionProperties = sections.get(targetSection).getSectPr();
	// There is always a section wrapper, but it might not contain a sectPr
	if (sectionProperties == null) {
	    sectionProperties = factory.createSectPr();
	    template.getMainDocumentPart().addObject(sectionProperties);
	    sections.get(targetSection).setSectPr(sectionProperties);
	}

	FooterReference footerReference = factory.createFooterReference();
	footerReference.setId(relationship.getId());
	footerReference.setType(type);
	sectionProperties.getEGHdrFtrReferences().add(footerReference);

	BooleanDefaultTrue value = new BooleanDefaultTrue();
	value.setVal(Boolean.TRUE);
	sectionProperties.setTitlePg(value);
    }

    private void createHeaderReference(Relationship relationship, HdrFtrRef type) {
	List<SectionWrapper> sections = template.getDocumentModel().getSections();

	SectPr sectionProperties = sections.get(targetSection).getSectPr();
	// There is always a section wrapper, but it might not contain a sectPr
	if (sectionProperties == null) {
	    sectionProperties = factory.createSectPr();
	    template.getMainDocumentPart().addObject(sectionProperties);
	    sections.get(targetSection).setSectPr(sectionProperties);
	}

	HeaderReference reference = factory.createHeaderReference();
	reference.setId(relationship.getId());
	reference.setType(type);
	sectionProperties.getEGHdrFtrReferences().add(reference);

	BooleanDefaultTrue value = new BooleanDefaultTrue();
	value.setVal(Boolean.TRUE);
	sectionProperties.setTitlePg(value);
    }

    private P createImageParagraph(String content, byte[] imageData, FooterPart footerPart) throws Exception {
	P paragraph = factory.createP();
	R run = factory.createR();
	Text text = new Text();
	text.setValue(content);
	run.getContent().add(text);

	PPr otherProperties = factory.createPPr();
	Jc jc = factory.createJc();
	jc.setVal(JcEnumeration.RIGHT);
	otherProperties.setJc(jc);
	paragraph.setPPr(otherProperties);
	//

	if (imageData != null) {
	    run.getContent().add(newImage(footerPart, imageData, "filename", "alttext", 1, 2));
	}

	paragraph.getContent().add(run);
	return paragraph;
    }

    private Ftr createFooter(String content, byte[] imageData, FooterPart footerPart) throws Exception {
	Ftr footer = factory.createFtr();
	P paragraph = createImageParagraph(content, imageData, footerPart);
	footer.getContent().add(paragraph);
	return footer;
    }

    private void updateFooterPart(FooterPart footerPart, String text, byte[] imageData) throws Exception {
	debug("Part name {0}", footerPart.getPartName().getName());
	footerPart.setJaxbElement(createFooter(text, imageData, footerPart));
    }

    public org.docx4j.wml.P newImage(Part sourcePart, byte[] bytes, String filenameHint, String altText, int id1,
	    int id2) throws Exception {
	BinaryPartAbstractImage imagePart = BinaryPartAbstractImage.createImagePart(template, sourcePart, bytes);
	Inline inline = imagePart.createImageInline(filenameHint, altText, id1, id2, false);
	// Now add the inline in w:p/w:r/w:drawing
	P p = factory.createP();
	R run = factory.createR();
	p.getContent().add(run);
	Drawing drawing = factory.createDrawing();
	run.getContent().add(drawing);
	drawing.getAnchorOrInline().add(inline);
	return p;
    }

    protected void debug(String message, Object... params) {
	String string = MessageFormat.format(message, params);
	System.out.println(string);
	// DfLogger.debug(this, message, params, null);
    }

}
