package ro.planet.documentum.stada.modules.pdf;

import static ro.planet.documentum.stada.modules.pdf.FieldSpecUtils.DMS_PROP_RREFIX;
import static ro.planet.documentum.stada.modules.pdf.FieldSpecUtils.DMS_PROP_RREFIX_UTIL;
import static ro.planet.documentum.stada.modules.pdf.FieldSpecUtils.PROP_RREFIX_UTIL;
import static ro.planet.documentum.stada.modules.pdf.FieldSpecUtils.fromRowSpecToSourceSpec;
import static ro.planet.documentum.stada.modules.pdf.FieldSpecUtils.getFieldSpec;
import static ro.planet.documentum.stada.modules.pdf.FieldSpecUtils.parseFieldSpec;
import static ro.planet.documentum.stada.modules.pdf.FieldSpecUtils.removeLastRowNumber;
import static ro.planet.documentum.stada.modules.pdf.FieldSpecUtils.specIsNumber;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.transform.TransformerException;

import org.docx4j.TraversalUtil;
import org.docx4j.XmlUtils;
import org.docx4j.docProps.custom.Properties.Property;
import org.docx4j.jaxb.Context;
import org.docx4j.model.fields.ComplexFieldLocator;
import org.docx4j.model.fields.FieldRef;
import org.docx4j.model.fields.FldSimpleModel;
import org.docx4j.model.fields.FormattingSwitchHelper;
import org.docx4j.model.fields.SimpleFieldLocator;
import org.docx4j.model.fields.merge.DataFieldName;
import org.docx4j.openpackaging.exceptions.Docx4JException;
import org.docx4j.openpackaging.packages.WordprocessingMLPackage;
import org.docx4j.openpackaging.parts.DocPropsCustomPart;
import org.docx4j.openpackaging.parts.Part;
import org.docx4j.openpackaging.parts.Parts;
import org.docx4j.openpackaging.parts.WordprocessingML.FooterPart;
import org.docx4j.openpackaging.parts.WordprocessingML.HeaderPart;
import org.docx4j.openpackaging.parts.WordprocessingML.MainDocumentPart;
import org.docx4j.wml.CTSimpleField;
import org.docx4j.wml.ContentAccessor;
import org.docx4j.wml.P;
import org.docx4j.wml.R;
import org.docx4j.wml.Tbl;
import org.docx4j.wml.Text;
import org.docx4j.wml.Tr;

import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfPersistentObject;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfId;

import ro.planet.documentum.stada.modules.word.ImageBaseHelper;
import ro.planet.documentum.stada.modules.word.WordFooterHelper;

/**
 * В процессы bdWSConvertContent и bdWSConvertMainConvent добавлены переменные.
 * <br>
 * 
 * in_main_content - Если не false, то контент помечается как основной. <br>
 * 
 * in_new_version Если true - создается новая версия. Если false - не создается
 * новая версия. Иначе, если шаблон содержит DOCPROPERTY, то создается новая
 * версия, иначе не создается. <br>
 * 
 * in_in_need_bacrode Если не false - впечатывается штрихкод.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TemplatePopulateHelper extends TemplatePopulateSourceHelper {

	private static final String TEMP_FILE_NAME = "TempMergeFieldDoc.docx";

	private List<String> tableKeywords = new ArrayList<String>();
	private Map<String, List<String>> tableKeywordsMap = new HashMap<String, List<String>>();
	private boolean deleteTempFile = true;
	private String barcodeURL = null;
	private TemplatePopulateAttrs attrs;
	private Set<String> docProperties = new HashSet<String>();
	private String resultFileName;
	private Map<String, String> numberFieldNames = new HashMap<String, String>();
	private String currentTemplateKey;

	private boolean hasDocFields = false;

	private IDfPersistentObject rootObject;

	private Date start = new Date();

	// Вообще говоря и с одним \u00A0 вроде работает, но поле короткое
	// получается.
	private static final String EMPTY_ROW_VALUE = "\u00A0 \u00A0";

	private boolean debugFieldsXml = false;

	public String executeWithSession(IDfSession session, String folderObjectId, String templateObjectId,
			String objectName) throws DfException {
		ITemplateSource source = new DefaultTemplateSource(session);
		TemplatePopulateAttrs attrs = new TemplatePopulateAttrs();
		attrs.setFolderObjectId(folderObjectId);
		attrs.setTemplateObjectId(templateObjectId);
		attrs.setObjectName(objectName);
		return executeWithSession(source, attrs);
	}

	public String executeWithSession(ITemplateSource source, String folderObjectId, String templateObjectId,
			String objectName) throws DfException {
		TemplatePopulateAttrs attrs = new TemplatePopulateAttrs();
		attrs.setFolderObjectId(folderObjectId);
		attrs.setTemplateObjectId(templateObjectId);
		attrs.setObjectName(objectName);
		return executeWithSession(source, attrs);
	}

	public String executeWithSession(IDfSession session, TemplatePopulateAttrs attrs) throws DfException {
		this.attrs = attrs;
		ITemplateSource source = new DefaultTemplateSource(session);
		return executeWithSession(source, attrs);
	}

	protected String executeWithSession(ITemplateSource templateSource, TemplatePopulateAttrs attrs)
			throws DfException {
		String returnId = null;
		this.attrs = attrs;
		this.session = templateSource;
		debug("template population, attrs: {0}", attrs);
		debug("template population, content: {0}", attrs.getContent());
		String objectName = attrs.getObjectName();
		String templateObjectId = attrs.getTemplateObjectId();
		String folderObjectId = attrs.getFolderObjectId();
		IDfSysObject templateObject = templateSource.getTemplateObject(new DfId(templateObjectId));
		returnId = templateObject.getObjectId().toString();
		if (DfId.isObjectId(folderObjectId) == false) {
			folderObjectId = templateObject.getFolderId(0).toString();
			attrs.setFolderObjectId(folderObjectId);
		}
		IDfPersistentObject folderObject = templateSource.getObject(new DfId(folderObjectId));
		boolean templatePatternFound = false;
		if (objectName.trim().length() == 0 || objectName.trim().equals("-")) {
			objectName = templateObject.getObjectName();
			String[] patterns = new String[] { "Шаблон", "-", "Template" };
			List<String> templatePatterns = Arrays.asList("Шаблон", "Template");
			boolean processed = true;
			while (processed) {
				processed = false;
				for (String pattern : patterns) {
					if (objectName.toLowerCase().startsWith(pattern.toLowerCase())) {
						objectName = objectName.substring(pattern.length()).trim();
						if (templatePatterns.contains(pattern)) {
							templatePatternFound = true;
						}
						processed = true;
						break;
					}
				}
			}
		}
		debug("found template pattern {0}", templatePatternFound);
		if (templatePatternFound == false) {
			if ("true".equals(attrs.getForce()) == false) {
				debug("skipping template modification for non template object");
				return returnId;
			}
		}
		if (templateObject.getContentSize() == 0) {
			debug("skipping template modification for contentless object");
		} else {
			String contentType = templateObject.getContentType();
			if ("msw12".equals(contentType) || "msw14".equals(contentType)) {
				returnId = updateTemplateEx(templateSource, folderObject, templateObject, objectName, returnId);
			} else {
				debug("Skipping template modification for content type {0}", contentType);
			}
		}
		return returnId;
	}

	private String updateTemplateEx(ITemplateSource templateSource, IDfPersistentObject folderObject,
			IDfSysObject templateObject, String objectName, String returnId) {
		this.rootObject = folderObject;
		// ByteArrayOutputStream newContent = null;
		ByteArrayInputStream content = null;
		File tempFile = null;
		// ClassLoader contextClassLoader =
		// Thread.currentThread().getContextClassLoader();
		try {
			// Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
			content = templateObject.getContent();
			debug("update template, content size {0}, time {1}", content.available(), getCurrentTime());
			WordprocessingMLPackage template = WordprocessingMLPackage.load(content);
			debug("template loaded, time {0}", getCurrentTime());
			initTemplateTables(template);
			debug("tables initialized, has doc fields {0}, time {1}", hasDocFields, getCurrentTime());
			Map<DataFieldName, String> replacementMap = createReplacementMap(template);
			Map<DataFieldName, String> rm = new HashMap<DataFieldName, String>();
			for (DataFieldName key : replacementMap.keySet()) {
				String val = replacementMap.get(key);
				// val = val.replaceAll("([a-zA-Zа-яА-Я0-9]{6})", "$1&#8203;");
				rm.put(key, val);
			}
			replacementMap = rm;
			debug("replacementMap: {0}", replacementMap);
			completeTable(template);
			boolean newVersion = true;
			if (hasDocFields) {
				updateDocFields(template, replacementMap);
			} else {
				// согласно запросу
				// https://ot-ps.ru/redmine/issues/18818
				// Дублируются вложения при редактировании карточки ИсхД (eroom
				// 469)
				// если нет полей типа properties, то ничего не делать
				return returnId;
				// MailMerger.performMerge(template, replacementMap, false);
				// newVersion = false;
			}
			debug("newVersion: {0}", newVersion);
			// newContent = getParsedContent(folderObject, content);
			IDfSysObject newObject = templateSource.getTargetObject(templateObject, folderObject, objectName, attrs,
					newVersion);
			IDfSession sessionForBarCode = templateSource.getDfSession();
			boolean needBarcode = !"false".equals(attrs.getNeedBarcode());
			debug("needBarcode: {0}, time: {1}", needBarcode, getCurrentTime());
			String uid = "ST0" + newObject.getObjectId().toString();
			if (sessionForBarCode != null && needBarcode == true) {
				byte[] image = new ImageBaseHelper().downloadBarcode(uid, sessionForBarCode);
				if (image != null) {
					debug("generate barcode");
					new WordFooterHelper().addImageOnFirstPage(template, "", "", image);
				} else {
					debug("no barcode servlet configured");
				}
			} else if (barcodeURL != null) {
				byte[] image = new ImageBaseHelper().downloadFromUrl(Arrays.asList(barcodeURL), uid);
				if (image != null) {
					debug("generate barcode");
					new WordFooterHelper().addImageOnFirstPage(template, "", "", image);
				} else {
					debug("no barcode servlet configured");
				}
			} else {
				debug("no barcode servlet will be executed, session is null");
			}
			debug("barcore has been checked, time: {0} ms", getCurrentTime());

			processHTML(templateSource, template);
			tempFile = File.createTempFile(TEMP_FILE_NAME, ".docx");
			template.save(tempFile);
			resultFileName = tempFile.getAbsolutePath();
			debug("save target object, resultFileName: {0}, time: {1}", resultFileName, getCurrentTime());
			session.saveTargetObject(templateObject, newObject, tempFile, uid, attrs.getOwner());
			debug("object has been saved, id: {0}", newObject.getObjectId());
			debug("word generation done, time: {0} ms", getCurrentTime());
			return newObject.getObjectId().toString();
		} catch (Exception ex) {
			error("error during template population", ex);
		} finally {
			// Thread.currentThread().setContextClassLoader(contextClassLoader);
			try {
				// if (newContent != null) newContent.close();
				if (content != null) {
					content.close();
				}
				if (tempFile != null) {
					if (deleteTempFile) {
						tempFile.delete();
					}
				}
			} catch (IOException ioex) {
				error("error during template close", ioex);
			}
		}
		return DfId.DF_NULLID_STR;
	}

	private long getCurrentTime() {
		return new Date().getTime() - start.getTime();
	}

	/**
	 * Пока ничего не делает, для отладки.
	 * 
	 * @param source
	 * @param result
	 * @param fields
	 */
	private void getFieldNamesComplex(Object source, List<String> result, List<Object> fields) {
		ComplexFieldLocator fl = new ComplexFieldLocator();
		new TraversalUtil(source, fl);
		// canonicalise and setup fieldRefs
		for (P p : fl.getStarts()) {
			/*
			 * Копируем, т. к. canonicalise меняет объект. В случае если это
			 * MERGEFIELD, то будет что-то не то.
			 */
			List<FieldRef> workingFieldRefs = new ArrayList<FieldRef>();
			P workingCopy = (P) XmlUtils.deepCopy(p);
			P newWorkingCopy;

			try {
				newWorkingCopy = canonicalise(workingCopy, workingFieldRefs);
			} catch (Throwable tr) {
				continue;
			}
			fields.add(p);
			fields.add("Cannonical form:");
			fields.add(newWorkingCopy);
		}
	}

	private List<String> getFieldNamesFromObjectList(List<Part> source) {
		List<String> results = new ArrayList<>();
		for (Object item : source) {
			results.addAll(getFieldNamesFromObject(item));
		}
		return results;
	}

	private List<String> getFieldNamesFromObject(Object source) {
		List<String> result = new ArrayList<String>();
		List<Object> fields = new ArrayList<Object>();

		List<Object> mergeElements = getAllElementFromObject(source, CTSimpleField.class);
		List<Object> textElements = getAllElementFromObject(source, Text.class);
		for (Object object : mergeElements) {
			CTSimpleField mergeF = (CTSimpleField) object;
			String oldValue = mergeF.getInstr();
			result.add(oldValue);
			fields.add(mergeF);
		}

		List<FieldRef> fieldRefs = getFieldRefs(source, false);
		for (FieldRef ref : fieldRefs) {
			String instr = extractInstr(ref.getInstructions());
			result.add(instr);
		}

		trace("fields intermediate: {0}", toDebugString(result));

		SimpleFieldLocator fl = new SimpleFieldLocator();
		new TraversalUtil(source, fl);
		for (CTSimpleField simpleField : fl.simpleFields) {
			if (result.contains(simpleField.getInstr()) == false) {
				result.add(simpleField.getInstr());
				fields.add(simpleField);
			}
		}

		getFieldNamesComplex(source, result, fields);

		trace("fields with traversal: {0}", toDebugString(result));

		if (debugFieldsXml) {
			trace("simple fields xml: {0}", toDebugString(fields));
		}

		for (Object object : textElements) {
			Text mergeF = (Text) object;
			String oldValue = mergeF.getValue();
			result.add(oldValue);
		}

		List<String> resultEx = new ArrayList<String>();
		for (String item : result) {
			if (supportedInstruction(item)) {
				resultEx.add(item);
			}
		}

		trace("fields, result: {0}", toDebugString(resultEx));
		return resultEx;
	}

	private List<String> getFieldSpecsFromObject(Object source) {
		List<String> values = getFieldNamesFromObject(source);
		List<String> result = new ArrayList<String>();
		for (String item : values) {
			String fieldSpecStr = getFieldSpec(item);
			result.add(fieldSpecStr);
		}
		return result;
	}

	private Map<DataFieldName, String> createReplacementMap(WordprocessingMLPackage template) throws Exception {

		Map<DataFieldName, String> replacementMap = new HashMap<DataFieldName, String>();

		List<String> fieldNames = getFieldNamesFromObjectList(getParts(template));
		for (String oldValue : fieldNames) {
			processMapEntries(replacementMap, oldValue, template);
		}

		boolean debugFields = false;
		if (debugFields) {
			SimpleFieldLocator fl = new SimpleFieldLocator();
			new TraversalUtil(template.getMainDocumentPart().getContent(), fl);
			for (CTSimpleField simpleField : fl.simpleFields) {
				debug("CTSimpleField, instr: {0}", simpleField.getInstr());
			}
		}

		return replacementMap;
	}

	private List<String> getFromTableKeywordsMap(String item) {
		List<String> items = tableKeywordsMap.get(item);
		if (items == null) {
			items = new ArrayList<String>();
			tableKeywordsMap.put(item, items);
		}
		return items;
	}

	private void addNumbers(Map<DataFieldName, String> replacementMap, int required) {

		for (String item : numberFieldNames.keySet()) {
			List<String> items = getFromTableKeywordsMap(item);

			int newRequired = items.size() + required;

			while (items.size() < newRequired) {
				int j = items.size();
				String name = fromSourceSpecToRowSpec(item, (j + 1));
				replacementMap.put(new DataFieldName(name), "" + (j + 1));
				items.add(name);
			}

			debug("Numbers size {0}", items.size());
		}
	}

	private boolean supportedInstruction(String value) {
		if (value.contains("MERGEFIELD") || value.contains("DOCPROPERTY")) {
			String spec = getFieldSpec(value);
			if (spec.trim().length() == 0) {
				return false;
			}
			if (value.contains("DOCPROPERTY")) {
				if ((spec.startsWith(DMS_PROP_RREFIX) == false) && (spec.startsWith(DMS_PROP_RREFIX_UTIL) == false)) {
					// debug("Invalid DOCPROPERTY Spec {0}", spec);
					return false;
				}
			}
			return true;
		}
		return false;
	}

	private void processMapEntries(Map<DataFieldName, String> replacementMap, String value,
			WordprocessingMLPackage template) throws Exception {
		if (supportedInstruction(value)) {
			trace("process field: {0}", value);

			String fieldSpec = getFieldSpec(value);
			if (fieldSpec.trim().length() == 0) {
				debug("field name cannot be obtained");
				return;
			}

			trace("fieldSpec: {0}", fieldSpec);

			String fieldName = fieldSpec;

			if (value.contains("DOCPROPERTY")) {
				docProperties.add(fieldSpec);
			}

			String decodedFieldName = URLDecoder.decode(fieldName, "Cp1251");

			String decodedFieldNameWithoutPrefix = checkFieldNamePrefix(decodedFieldName).getData();
			if (decodedFieldNameWithoutPrefix.length() == 0) {
				return;
			}

			if (specIsNumber(decodedFieldNameWithoutPrefix)) {
				trace("processing number field, decodedFieldNameWithoutPrefix: {0}", decodedFieldNameWithoutPrefix);

				if (findTemplateTable(template, fieldName) != null) {
					trace("table found for the number field");

					tableKeywords.add(fieldName);
					numberFieldNames.put(fieldName, decodedFieldNameWithoutPrefix);
					addNumbers(replacementMap, 0);
				} else {
					trace("table did not found for the number field");
				}
				return;
			}

			List<String> items;

			Object fieldValue = getAttrValue(rootObject, decodedFieldName, true);
			if (fieldValue == null) {
				debug("skip field, decodedFieldName: {0}, no data found", decodedFieldName);
				return;
			}

			items = checkList(fieldValue, true);

			if (findTemplateTable(template, fieldName) != null) {
				tableKeywords.add(fieldName);
				debug("found table key word, fieldName: {0}, decodedFieldName: {1}, size: {2}", fieldName,
						decodedFieldName, items.size());

				List<String> tableItems = new ArrayList<String>(items);
				if (tableItems.size() == 0) {
					tableItems.add(EMPTY_ROW_VALUE);
				}

				if (getFromTableKeywordsMap(fieldSpec).size() == 0) {

					int j = 0;
					for (String item : tableItems) {
						String name = fromSourceSpecToRowSpec(fieldSpec, (j + 1));
						if (item.trim().length() == 0) {
							item = EMPTY_ROW_VALUE;
						}
						replacementMap.put(new DataFieldName(name), item);
						trace("replacement, fieldSpec: {0}, name: {1}, item: {2}", fieldSpec, name, item);
						j++;

						getFromTableKeywordsMap(fieldSpec).add(name);
					}

				}

				addNumbers(replacementMap, tableItems.size());
			}

			trace("process as not table field, fieldName: {0}, decodedFieldName: {1}", fieldName, decodedFieldName);

			String str = listToString(items);
			if (str.trim().length() == 0) {
				str = EMPTY_ROW_VALUE;
			}
			replacementMap.put(new DataFieldName(fieldSpec), str);

			trace("replacement, fieldSpec: {0}, str: {1}", fieldSpec, str);
		}
	}

	private void completeTable(WordprocessingMLPackage template) throws Exception {
		List<Object> tables = getAllElementFromObject(getParts(template), Tbl.class, "DMSxCP.skip");

		for (Object tbl : tables) {
			Tbl tempTable = getTemplateTable(tbl);
			if (tempTable == null) {
				continue;
			}
			List<String> items = getFromTableKeywordsMap(currentTemplateKey);
			int rowNr = items.size();
			debug("Current template key {0}, count {1}", currentTemplateKey, rowNr);

			List<Object> rows = getAllElementFromObject(tempTable, Tr.class);

			debug("Rows in the table {0}", rows.size());

			if (rows.size() == 0) {
				debug("Skip table with 0 rows");
				continue;
			}

			Tr templateRow = getFirstRow(tempTable, true);
			if (templateRow == null) {
				debug("Skip table with no first row");
				continue;
			}

			for (int rowIndex = 1; rowIndex <= rowNr; rowIndex++) {
				addRowToTable(template, tempTable, templateRow, rowIndex, items);
			}

			tempTable.getContent().remove(templateRow);

		}
	}

	private Tbl getTemplateTable(Object tbl) throws Docx4JException, JAXBException {
		List<String> textElements = getFieldNamesFromObject(tbl);

		for (String text : textElements) {
			String spec = getFieldSpec(text);
			if (spec.length() > 0) {
				if (tableKeywords.contains(spec) && (numberFieldNames.containsKey(spec) == false)) {
					currentTemplateKey = spec;
					return (Tbl) tbl;
				}
			}
		}

		return null;
	}

	private boolean fieldHasValue(String value, String requiredValue) {
		if (supportedInstruction(value)) {
			String fieldName = getFieldSpec(value);
			return fieldName.trim().equalsIgnoreCase(requiredValue.trim());
		}
		return false;
	}

	private Tr getFirstRow(Tbl tbl, boolean removeNonFirstRows) throws Exception {
		Tr firstRow = null;

		List<Object> rows = getAllElementFromObject(tbl, Tr.class);
		for (Object rowObj : rows) {
			List<String> fieldNames = getFieldNamesFromObject(rowObj);
			if (fieldNames.size() > 0) {
				if (firstRow == null) {
					firstRow = (Tr) rowObj;
				} else {
					if (removeNonFirstRows) {
						tbl.getContent().remove(rowObj);
					}
				}
			}
		}
		return firstRow;
	}

	private void initTemplateTables(WordprocessingMLPackage template) throws Exception {
		debug("initialize tables");

		DocPropsCustomPart docPropsCustomPart = template.getDocPropsCustomPart();
		if (docPropsCustomPart != null) {
			List<Property> propList = docPropsCustomPart.getContents().getProperty();
			for (Property item : propList) {
				if (item.getName().startsWith(DMS_PROP_RREFIX)) {
					hasDocFields = true;
					break;
				}
			}
		}

		List<Object> tables = getAllElementFromObject(getParts(template), Tbl.class, "DMSxCP.skip");
		for (Object tblObj : tables) {

			Tbl tbl = (Tbl) tblObj;
			Object firstRow = null;

			List<Object> rows = getAllElementFromObject(tbl, Tr.class);
			for (Object rowObj : rows) {
				List<String> fieldNames = getFieldNamesFromObject(rowObj);
				if (fieldNames.size() > 0) {
					if (firstRow == null) {
						firstRow = rowObj;
					} else {
						tbl.getContent().remove(rowObj);
					}
				}
			}

			if (firstRow != null) {
				updateInstructions(firstRow, new IStringProcessor() {

					@Override
					public String process(String oldValue) {
						return removeMergeFieldCounter(oldValue);
					}
				});
			}
		}

		docPropsCustomPart = template.getDocPropsCustomPart();
		if (docPropsCustomPart != null) {
			List<Property> propList = docPropsCustomPart.getContents().getProperty();
			List<Property> removeProp = new ArrayList<Property>();
			for (Property prop : propList) {
				String name = prop.getName();
				if (name.startsWith(DMS_PROP_RREFIX) || name.startsWith(DMS_PROP_RREFIX_UTIL)) {
					if (removeLastRowNumber(name).equals(name) == false) {
						debug("remove property: {0}", name);
						removeProp.add(prop);
					}
				}
			}

			for (Property prop : removeProp) {
				propList.remove(prop);
			}
		}

		initCurrentPropertyNames(template);
	}

	/**
	 * Сохраняет все имена свойств в массиве, для последующей проверки на
	 * дубликаты. Чтобы новые вспомогательные свойства не пересекались с
	 * предыдущими.
	 * 
	 * @param template
	 * @throws Exception
	 */
	private void initCurrentPropertyNames(WordprocessingMLPackage template) throws Exception {
		DocPropsCustomPart docPropsCustomPart;

		docPropsCustomPart = template.getDocPropsCustomPart();
		if (docPropsCustomPart != null) {
			List<Property> propList = docPropsCustomPart.getContents().getProperty();

			for (Property prop : propList) {
				String name = prop.getName();
				currentPropertyNames.add(name);
			}

		}

		debug("Current property names {0}", Arrays.toString(currentPropertyNames.toArray()));
	}

	private Tbl findTemplateTable(WordprocessingMLPackage template, String templateKey)
			throws Docx4JException, JAXBException {
		List<Object> tables = getAllElementFromObject(getParts(template), Tbl.class, "DMSxCP.skip");
		for (Object tbl : tables) {

			List<String> fieldNames = getFieldNamesFromObject(tbl);
			for (String oldValue : fieldNames) {
				if (oldValue != null && fieldHasValue(oldValue, templateKey)) {
					debug("Value for {0} = {1}", templateKey, oldValue);
					return (Tbl) tbl;
				}
			}
		}
		return null;
	}

	private interface IStringProcessor {
		public String process(String item);
	}

	boolean debugUpdateInstructions = true;

	private void updateInstructions(Object workingRow, IStringProcessor processor) {
		List<Object> mergeElements = getAllElementFromObject(workingRow, CTSimpleField.class);
		for (Object object : mergeElements) {
			CTSimpleField mergeF = (CTSimpleField) object;
			String oldValue = mergeF.getInstr();
			String newValue = processor.process(oldValue);
			if (!(newValue == null || newValue.length() == 0) && !newValue.equals(oldValue)) {
				if (debugUpdateInstructions) {
					debug("Update instructions\r\n{0}\r\n{1}", oldValue, newValue);
				}

				mergeF.setInstr(newValue);
			}
		}

		// Required for merge fields.
		boolean processTextFields = true;
		if (processTextFields) {
			List<Object> textElements = getAllElementFromObject(workingRow, Text.class);
			for (Object object : textElements) {
				Text mergeF = (Text) object;
				String oldValue = mergeF.getValue();
				String newValue = processor.process(oldValue);
				if (!(newValue == null || newValue.length() == 0) && !newValue.equals(oldValue)) {
					if (debugUpdateInstructions) {
						debug("Update instructions (text)\r\n{0}\r\n{1}", oldValue, newValue);
					}

					mergeF.setValue(newValue);
				}
			}
		}

		List<FieldRef> fieldRefs = getFieldRefs(workingRow, true);
		for (FieldRef ref : fieldRefs) {
			String oldValue = extractInstr(ref.getInstructions());
			String newValue = processor.process(oldValue);

			if (!(newValue == null || newValue.length() == 0) && !newValue.equals(oldValue)) {
				if (debugUpdateInstructions) {
					debug("Update instructions (complex)\r\n{0}\r\n{1}", oldValue, newValue);
				}

				List<Object> instr = ref.getInstructions();
				List<Text> text = new ArrayList<Text>();
				List<Object> objects = new ArrayList<Object>();
				for (int i = 0; i < instr.size(); i++) {
					Object obj = instr.get(i);
					Object unwrapObj = XmlUtils.unwrap(obj);

					if (unwrapObj instanceof Text) {
						text.add((Text) unwrapObj);
						objects.add(obj);
					}
				}

				debug("Text found {0}", text.size());

				if (text.size() > 0) {
					int fragmentLength = newValue.length() / text.size();

					boolean useFirst = true;
					if (useFirst) {
						debug("Set text value {0}", newValue);

						Text first = text.get(0);
						first.setValue(newValue);
						for (int i = 1; i < text.size(); i++) {
							Text item = text.get(i);
							Object obj = item.getParent();
							item.setValue(" ");
							debug("Text parent {0}", obj.getClass().getName());
							if (obj instanceof ContentAccessor) {
								ContentAccessor contentAccessor = (ContentAccessor) obj;
								boolean removed = contentAccessor.getContent().remove(item);
								debug("Remove item {0}, {1}, list count {2}", i, removed,
										contentAccessor.getContent().size());
							}
						}
					} else {
						String str;
						for (int i = 0; i < text.size(); i++) {
							if (i != (text.size() - 1)) {
								str = newValue.substring(0, fragmentLength);
								newValue = newValue.substring(fragmentLength);
							} else {
								str = newValue;
							}
							debug("Set text value {0}", str);
							text.get(i).setValue(str);
						}
					}
				}
			}
		}
	}

	private void addRowToTable(WordprocessingMLPackage template, Tbl reviewtable, Tr templateRow, final int rowIndex,
			final List<String> items) throws Docx4JException {

		debug("Add row to table {0}", rowIndex);

		Tr workingRow = (Tr) XmlUtils.deepCopy(templateRow);

		updateInstructions(workingRow, new IStringProcessor() {

			@Override
			public String process(String oldValue) {
				return appendMergeFieldCounter(oldValue, rowIndex);
			}
		});

		reviewtable.getContent().add(workingRow);
	}

	private int rowSpecIndex = 0;

	private String fromSourceSpecToRowSpec(String oldValue, int rowIndex) {
		if (oldValue.startsWith(DMS_PROP_RREFIX)) {
			boolean isNumber = numberFieldNames.containsKey(oldValue);
			if (isNumber == false) {
				if (rowIndex > 1) {
					rowSpecIndex++;
					return DMS_PROP_RREFIX_UTIL + "_" + rowSpecIndex;
				}
			}
		}

		String result = oldValue + "_" + rowIndex;
		if (result.startsWith(DMS_PROP_RREFIX)) {
			result = result.substring(DMS_PROP_RREFIX.length());
			result = DMS_PROP_RREFIX_UTIL + result;
		} else {
			result = PROP_RREFIX_UTIL + result;
		}
		return result;
	}

	private Map<String, Integer> numCounters = new HashMap<String, Integer>();

	private int getNextNumCounter(String numStr) {
		int result = 0;
		if (numCounters.containsKey(numStr)) {
			result = numCounters.get(numStr);
		}
		result++;
		numCounters.put(numStr, result);
		return result;
	}

	/**
	 * Преобразует из исходного имени в вспомогательную переменную. <br>
	 * Данный метод для одного и того же field-a может вызываться 2 раза. (Из-за
	 * того, что идет обработка простых field-ов, затем текстовых полей, и
	 * сложных field-ов). Поэтому, нужно учитывать, что на вход будет идти уже
	 * вспомогательное имя, которое не нужно менять (проверка на
	 * startWith(DMS_PROP_RREFIX_UTIL)). <br>
	 * Целевое значение для поля таблицы определяется в методе
	 * processMapEntries. В текущем методе оно извлекается из массива
	 * tableKeywordsMap.
	 * 
	 * @param oldValue
	 * @param rowIndex
	 * @return
	 */
	private String appendMergeFieldCounter(String oldValue, int rowIndex) {
		debug("Append field counter {0}, {1}", oldValue, rowIndex);
		if (supportedInstruction(oldValue)) {
			FieldSpec spec = parseFieldSpec(oldValue);
			if (!spec.parsed()) {
				return null;
			}

			/**
			 * Вспомогательная временная переменная.
			 */
			if (spec.data.startsWith(DMS_PROP_RREFIX_UTIL)) {
				return null;
			}
			if (spec.data.startsWith(PROP_RREFIX_UTIL)) {
				return null;
			}

			List<String> items = getFromTableKeywordsMap(spec.data);
			if (numberFieldNames.containsKey(spec.data)) {
				String numStr = numberFieldNames.get(spec.data);
				if (numStr.equals("#") == false) {
					rowIndex = getNextNumCounter(numStr);
				}
			}

			int row = rowIndex - 1;
			spec.data = row < items.size() ? items.get(rowIndex - 1) : fromSourceSpecToRowSpec(spec.data, rowIndex);
			return spec.toString();
		}
		return null;
	}

	private String removeMergeFieldCounter(String oldValue) {
		if (supportedInstruction(oldValue)) {
			FieldSpec spec = parseFieldSpec(oldValue);
			if (!spec.parsed()) {
				return null;
			}
			spec.data = fromRowSpecToSourceSpec(spec.data);
			return spec.toString();
		}
		return null;
	}

	private List<Object> getAllElementFromObject(Object obj, Class<?> toSearch) {
		return getAllElementFromObject(obj, toSearch, null);
	}

	private List<Object> getAllElementFromObject(Object obj, Class<?> toSearch, String excludeMark) {
		List<Object> result = new ArrayList<Object>();

		if (obj instanceof List) {
			List<Object> source = (List<Object>) obj;
			for (Object item : source) {
				result.addAll(getAllElementFromObject(item, toSearch, excludeMark));
			}
			return result;
		}

		// JBoss содержит модуль со своей реализацией JAXB и класс JAXBElement загружается
		// ClassLoader'ом модуля, поэтому в данном месте obj instanceof JAXBElement вернёт false
		// даже если obj имеет тип JAXBElement
		if (obj.getClass().getName().equals(JAXBElement.class.getName())) {
			if (obj instanceof JAXBElement) {
				obj = ((JAXBElement<?>) obj).getValue();
			} else {
				try {
					Method method = obj.getClass().getDeclaredMethod("getValue");
					obj = method.invoke(obj);
				} catch (Exception e) {
					error("Error during 'getValue' method invocation", e);
				}
			}
		}

		if (obj.getClass().equals(toSearch)) {

			boolean haveExcludeMark = false;
			if (excludeMark != null) {
				if (marshaltoDebugString(obj).contains(excludeMark)) {
					haveExcludeMark = true;
				}
			}

			if (haveExcludeMark == false) {
				result.add(obj);
				return result;
			}
		}

		if (obj instanceof ContentAccessor) {
			List<?> children = ((ContentAccessor) obj).getContent();
			for (Object child : children) {
				result.addAll(getAllElementFromObject(child, toSearch, excludeMark));
			}

		}
		return result;
	}

	public void setDeleteTempFile(boolean value) {
		deleteTempFile = value;
	}

	public String getBarcodeURL() {
		return barcodeURL;
	}

	public void setBarcodeURL(String barcodeURL) {
		this.barcodeURL = barcodeURL;
	}

	private void updateDocFields(WordprocessingMLPackage template, Map<DataFieldName, String> map) throws Exception {
		updateSimple(getParts(template), map);
		updateComplex(getParts(template), map);
	}

	public String getResultFileName() {
		return resultFileName;
	}

	private String getFieldValue(Map<DataFieldName, String> map, String key) {
		if (key.equalsIgnoreCase("DMSxCP.skip")) {
			return " ";
		}
		DataFieldName field = new DataFieldName(key);
		String val = map.get(field);
		return val;
	}

	public void updateSimple(List<Part> part, Map<DataFieldName, String> map) throws Docx4JException {
		for (Part item : part) {
			updateSimple(item, map);
		}
	}

	/**
	 * from FieldUpdater
	 * 
	 * @param part
	 * @throws Docx4JException
	 */
	public void updateSimple(Part part, Map<DataFieldName, String> map) throws Docx4JException {

		FldSimpleModel fsm = new FldSimpleModel(); // gets reused
		List contentList = ((ContentAccessor) part).getContent();
		WordprocessingMLPackage wmlPackage = (WordprocessingMLPackage) part.getPackage();

		// find fields
		SimpleFieldLocator fl = new SimpleFieldLocator();
		new TraversalUtil(contentList, fl);

		debug("simple fields in {0}", part.getPartName());
		debug("found {0} simple fields", fl.simpleFields.size());

		for (CTSimpleField simpleField : fl.simpleFields) {

			debug("update simple field: {0}", toDebugString(simpleField));

			if ("DOCPROPERTY".equals(FormattingSwitchHelper.getFldSimpleName(simpleField.getInstr()))) {
				// only parse those fields that get processed
				try {
					fsm.build(simpleField.getInstr());
				} catch (TransformerException e) {
					e.printStackTrace();
				}

				String key = fsm.getFldParameters().get(0);

				String val;

				val = getFieldValue(map, key);

				if (val == null) {
					warning("not found: {0} -> {1}", simpleField.getInstr(), key);
					val = "";

					/*
					 * Если данных нет, не меняем Field.
					 */
					continue;
				}

				// docPropsCustomPart.getProperty(key);
				val = FormattingSwitchHelper.applyFormattingSwitch(wmlPackage, fsm, val);
				debug("set {0} to {1}", simpleField.getInstr(), val);

				R r = null;
				if (simpleField.getInstr().toUpperCase().contains("MERGEFORMAT")) {
					// find the first run and use the formatting of that
					r = getFirstRun(simpleField.getContent());
				}
				if (r == null) {
					r = Context.getWmlObjectFactory().createR();
				} else {
					r.getContent().clear();
				}
				simpleField.getContent().clear();
				simpleField.getContent().add(r);
				Text t = Context.getWmlObjectFactory().createText();
				t.setValue(val);
				// t.setSpace(value) //TODO
				r.getContent().add(t);

				if (key.startsWith(DMS_PROP_RREFIX) || key.startsWith(DMS_PROP_RREFIX_UTIL)) {
					DocPropsCustomPart docPropsCustomPart = wmlPackage.getDocPropsCustomPart();
					docPropsCustomPart.setProperty(key, val);
				}

			} else {

				warning("ignoring {0}", simpleField.getInstr());

			}
		}

	}

	/**
	 * Возвращает список FieldRef, каждый из которых соответствует вставленной
	 * переменной в передаваемой области. <br>
	 * Если cannonicalize = true - то текущий документ меняется. С таким
	 * параметром можно вызывать, только при обновлении полей. Если вызывается
	 * для получения информации о полях, например, для построения списка
	 * инструкций, вызывать с cannonicalize = false.
	 * 
	 * @param object
	 * @param cannonicalize
	 * @return
	 */
	private List<FieldRef> getFieldRefs(Object object, boolean cannonicalize) {

		List<FieldRef> fieldRefs = new ArrayList<FieldRef>();

		if (object instanceof ContentAccessor) {
			List contentList = ((ContentAccessor) object).getContent();

			ComplexFieldLocator fl = new ComplexFieldLocator();
			new TraversalUtil(contentList, fl);

			trace("Found {0} fields", fl.getStarts().size());

			// canonicalise and setup fieldRefs

			for (P p : fl.getStarts()) {

				/*
				 * Копируем, т. к. canonicalise меняет объект. В случае если это
				 * MERGEFIELD, то будет что-то не то.
				 */
				List<FieldRef> workingFieldRefs = new ArrayList<FieldRef>();
				P workingCopy = (P) XmlUtils.deepCopy(p);
				try {
					P newWorkingCopy = canonicalise(workingCopy, workingFieldRefs);
				} catch (Throwable tr) {
					continue;
				}

				boolean hasDocField = false;

				for (FieldRef ref : workingFieldRefs) {

					String instr = extractInstr(ref.getInstructions());

					FieldSpec spec = parseFieldSpec(instr);

					if (spec.isDocProperty()) {
						if (spec.data.startsWith(DMS_PROP_RREFIX) || spec.data.startsWith(DMS_PROP_RREFIX_UTIL)) {
							hasDocField = true;
						} else {
							debug("Is not supported doc property {0}", spec);
						}
					} else {
						debug("Is not doc property {0}", spec);
					}

				}

				if (hasDocField == false) {
					continue;
				}

				if (!cannonicalize) {
					fieldRefs.addAll(workingFieldRefs);
					continue;
				}

				/**
				 * Новый метод canonicalise решает проблему сохранения шрифта
				 * при обновлении поля данных.
				 */

				int index;
				if (p.getParent() instanceof ContentAccessor) {
					index = ((ContentAccessor) p.getParent()).getContent().indexOf(p);
					P newP = canonicalise(p, fieldRefs);
					// log.debug("NewP length: " + newP.getContent().size() );
					((ContentAccessor) p.getParent()).getContent().set(index, newP);
				} else if (p.getParent() instanceof java.util.List) {
					// This does happen!
					index = ((List) p.getParent()).indexOf(p);
					P newP = canonicalise(p, fieldRefs);
					// log.debug("NewP length: " + newP.getContent().size() );
					((List) p.getParent()).set(index, newP);
				} else {
					warning("Unexpected parent: {0}", p.getParent().getClass().getName());
				}
			}
		}

		return fieldRefs;
	}

	public void updateComplex(List<Part> part, Map<DataFieldName, String> map) throws Docx4JException {
		for (Part item : part) {
			updateComplex(item, map);
		}
	}

	private void updateComplex(Part part, Map<DataFieldName, String> map) throws Docx4JException {

		FldSimpleModel fsm = new FldSimpleModel(); // gets reused

		trace("complex fields in {0}", part.getPartName());

		WordprocessingMLPackage wmlPackage = (WordprocessingMLPackage) part.getPackage();

		List<FieldRef> fieldRefs = getFieldRefs(part, true);

		// Populate
		for (FieldRef fr : fieldRefs) {

			trace("complex field: {0}", fr.getFldName());

			if ("DOCPROPERTY".equals(fr.getFldName())) {
				String instr = extractInstr(fr.getInstructions());
				try {
					fsm.build(instr);
				} catch (TransformerException e) {
					error("fsm build failed", e);
				}

				if (fsm.getFldParameters().size() == 0) {
					debug("cannot found field parameters for {0}", instr);
					return;
				}
				String key = fsm.getFldParameters().get(0);

				String val = getFieldValue(map, key);

				if (val == null) {

					debug("not found: {0} -> {1}", key, instr);

					val = "";

					/*
					 * Если данных нет, не меняем Field.
					 */
					continue;
				}

				val = FormattingSwitchHelper.applyFormattingSwitch(wmlPackage, fsm, val);

				trace("{0}", instr);
				trace("set value of complex field {0} = {1}", key, val);

				setResult(fr, val);

				if (key.startsWith(DMS_PROP_RREFIX) || key.startsWith(DMS_PROP_RREFIX_UTIL)) {
					DocPropsCustomPart docPropsCustomPart = wmlPackage.getDocPropsCustomPart();
					docPropsCustomPart.setProperty(key, val);
				}

			} else {
				warning("ignoring {0}", fr.getFldName());
			}
		}
	}

	private boolean debugExtractInstr = false;

	private String extractInstr(List<Object> instructions) {
		trace("Instructions {0}", instructions.size());
		StringBuilder result = new StringBuilder();
		for (Object obj : instructions) {
			Object unwrapObj = XmlUtils.unwrap(obj);

			if (debugExtractInstr) {
				trace("{0}, {1}", unwrapObj == null ? "NULL" : unwrapObj.getClass().getName(),
						unwrapObj instanceof Text ? ((Text) unwrapObj).getValue() : "");
			}

			if (unwrapObj instanceof Text) {
				result.append(((Text) unwrapObj).getValue());

			} else {
				warning("Cannot extract field name from {0}", unwrapObj.getClass().getName());
				warning("{0}", XmlUtils.marshaltoString(instructions.get(0), true, true));
			}
		}

		String resultStr = result.toString();

		if (debugExtractInstr) {
			trace("extractInstr {0}", resultStr);
		}

		return resultStr;
	}

	private R getFirstRun(List<Object> content) {

		for (Object o : content) {
			if (o instanceof R)
				return (R) o;
		}
		return null;
	}

	public void setResult(FieldRef ref, String val) {
		R resultsSlot = ref.getResultsSlot();
		resultsSlot.getContent().clear();
		StringTokenizer st = new StringTokenizer(val, "\n\r\f"); // tokenize on
		// the newline
		// character,
		// the
		// carriage-return
		// character,
		// and the
		// form-feed
		// character

		// our docfrag may contain several runs
		boolean firsttoken = true;
		while (st.hasMoreTokens()) {
			String line = (String) st.nextToken();

			if (firsttoken) {
				firsttoken = false;
			} else {
				resultsSlot.getContent().add(Context.getWmlObjectFactory().createBr());
			}

			org.docx4j.wml.Text text = Context.getWmlObjectFactory().createText();
			resultsSlot.getContent().add(text);
			if (line.startsWith(" ") || line.endsWith(" ")) {
				// TODO: tab character?
				text.setSpace("preserve");
			}
			text.setValue(line);
		}
	}

	public boolean isRenditionExist(IDfSysObject sysObject, String renditionFormat) throws DfException {
		boolean result = false;
		List<String> allFormats = getRenditionFormats(sysObject);
		if (allFormats != null && allFormats.size() > 0 && renditionFormat != null && renditionFormat.length() > 0) {
			result = allFormats.contains(renditionFormat);
		}
		return result;
	}

	public List<String> getRenditionFormats(IDfSysObject sysObject) throws DfException {
		IDfCollection renditions = null;
		List<String> renditionsFormatList = new ArrayList<String>();
		try {
			if (sysObject != null) {
				renditions = sysObject.getRenditions("full_format");
			}
			while (renditions != null && renditions.next() == true) {
				String currRenditionFormat = renditions.getString("full_format");
				renditionsFormatList.add(currRenditionFormat);
			}
		} finally {
			if (renditions != null) {
				renditions.close();
			}
		}
		return renditionsFormatList;
	}

	private void processHTML(ITemplateSource templateSource, WordprocessingMLPackage template) throws Exception {
		String idStr = attrs.getHtmlSourceId();
		if (idStr == null) {
			idStr = "";
		}

		InputStream contentInputStream = null;
		debug("process html ids: {0}", idStr);

		List<HTMLContentSource> list = new ArrayList<>();
		boolean hasData = false;

		String[] ids = idStr.split(",");
		for (String id : ids) {
			if (DfId.isObjectId(id) == true) {
				IDfPersistentObject pf = templateSource.getObject(new DfId(id));
				IDfSysObject sys = (IDfSysObject) pf;
				String format = "html";
				try {
					if (isRenditionExist(sys, format) == true) {
						debug("id {0} has format {1}", id, format);
						contentInputStream = sys.getContentEx(format, 0);
						list.add(new HTMLContentSource(contentInputStream));
						hasData = true;
					} else {
						debug("id {0} has no format {1}", id, format);
					}
				} catch (Throwable tr) {
					debug("cannot load {0} content from {1}, exception: {2}", format, id, tr.getMessage());
				}
				try {
					if (contentInputStream == null) {
						contentInputStream = sys.getContent();
						list.add(new HTMLContentSource(contentInputStream));
						hasData = true;
					}
				} catch (Throwable tr) {
					debug("cannot load content from {0}, exception: {1}", id, tr.getMessage());
				}
			} else {
				list.add(new HTMLContentSource());
			}
		}
		try {
			String body = attrs.getContent();
			if (contentInputStream == null && body != null && body.trim().length() > 0) {
				debug("use body content: {0}", body);
				contentInputStream = new ByteArrayInputStream(attrs.getContent().getBytes());
				list.add(new HTMLContentSource(contentInputStream));
				hasData = true;
			}
		} catch (Throwable tr) {
			debug("cannot use body content, bytes: {0}", attrs.getContent());
		}
		try {
			HTMLContentHelper htmlContentHelper = new HTMLContentHelper();
			if (hasData == false) {
				debug("skip update html content");
				// htmlContentHelper.update(template, "");
			} else {
				debug("update html content");
				boolean proceedBookmarks = "true".equalsIgnoreCase(attrs.getProceedBookmarks());
				boolean proceedForms = "true".equalsIgnoreCase(attrs.getProceedForms());
				debug("proceedBookmarks: {0}, proceedForms: {1}", proceedBookmarks, proceedForms);
				htmlContentHelper.update(template, proceedBookmarks, proceedForms,
						list.toArray(new HTMLContentSource[] {}));
			}
		} catch (Throwable tr) {
			error("cannot insert html content", tr);
		}

		debug("html content has been updated");
	}

	private List<Part> getParts(WordprocessingMLPackage template) {
		List<Part> results = new ArrayList<>();

		MainDocumentPart first = template.getMainDocumentPart();

		results.add(first);
		List<String> resultStr = new ArrayList<>();

		Parts parts = template.getParts();
		for (Part part : parts.getParts().values()) {
			// debug("{0}", part.getPartName());
			if (part == first) {
				debug("First part is found");
			} else if (part instanceof HeaderPart) {

				results.add(part);
			} else if (part instanceof FooterPart) {

				results.add(part);
			}
		}

		for (Part part : results) {
			resultStr.add(part.getPartName().toString());
		}
		debug("Part count {0}: {1}", results.size(), Arrays.toString(resultStr.toArray()));
		return results;
	}
}
