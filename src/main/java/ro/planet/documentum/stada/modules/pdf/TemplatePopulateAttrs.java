package ro.planet.documentum.stada.modules.pdf;

import java.text.MessageFormat;

/**
 * Параметры запуска метода генерации содержимого Word.
 */
public class TemplatePopulateAttrs {

	/**
	 * Целевая папка. Если не указано, определяется как первая папка шаблона
	 * (i_folder_id[0]).
	 */
	private String folderObjectId;

	/**
	 * ID объекта шаблона.
	 */
	private String templateObjectId;

	/**
	 * Если указано,определяет имя создаваемого файла.
	 */
	private String objectName;

	/**
	 * true- создаваемый объект помечается как основной, т. е. его ID
	 * прописывается в dss_maindoc_content кейза, и у создаваемого объекта
	 * dss_main_file присваивается признак основного контента "01".
	 */
	private String mainContent;

	/**
	 * true - при обработке создается новая версия. <br>
	 * false - при обработке создается новый документ.<br>
	 * иначе, определяется типом шаблона. Если шаблон использует docfield-ы, то
	 * true, иначе, false.
	 */
	private String newVersion;

	/**
	 * Если не false - создается штрих-код.
	 */
	private String needBarcode;

	/**
	 * Если не true, и документ не имеет признака шаблона в имени, то обработка
	 * не осуществляется. К примеру, признаком шаблона является слово "Шаблон" в
	 * начале имени документа. Документ с именем "Документ 1" не содержит
	 * признака шаблона в имени.
	 */
	private String force;

	/**
	 * Владелец создаваемых объектов. Не обязательное поле.
	 */
	private String owner;

	/**
	 * Если указано, используется в качестве контента HTML.
	 */
	private String htmlSourceId;

	/**
	 * true, если обрабатывать букмарки
	 */
	private String proceedBookmarks = "true";

	/**
	 * true, если обрабатывать формы
	 */
	private String proceedForms = "false";

	private String content = null;

	public String getFolderObjectId() {
		return folderObjectId;
	}

	public void setFolderObjectId(String folderObjectId) {
		this.folderObjectId = folderObjectId;
	}

	public String getTemplateObjectId() {
		return templateObjectId;
	}

	public void setTemplateObjectId(String templateObjectId) {
		this.templateObjectId = templateObjectId;
	}

	public String getObjectName() {
		return objectName;
	}

	public void setObjectName(String objectName) {
		this.objectName = objectName;
	}

	public String getMainContent() {
		return mainContent;
	}

	public void setMainContent(String mainContent) {
		this.mainContent = mainContent;
	}

	public String getNewVersion() {
		return newVersion;
	}

	public void setNewVersion(String newVersion) {
		this.newVersion = newVersion;
	}

	public String getNeedBarcode() {
		return needBarcode;
	}

	public void setNeedBarcode(String needBarcode) {
		this.needBarcode = needBarcode;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public String getForce() {
		return force;
	}

	public void setForce(String force) {
		this.force = force;
	}

	public String getHtmlSourceId() {
		return htmlSourceId;
	}

	public void setHtmlSourceId(String htmlSourceId) {
		this.htmlSourceId = htmlSourceId;
	}

	public String getProceedBookmarks() {
		return proceedBookmarks;
	}

	public void setProceedBookmarks(String proceedBookmarks) {
		this.proceedBookmarks = proceedBookmarks;
	}

	public String getProceedForms() {
		return proceedForms;
	}

	public void setProceedForms(String proceedForms) {
		this.proceedForms = proceedForms;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public String toString() {
		String str = MessageFormat.format(
				"folderObjectId: {0}, templateObjectId: {1}, objectName: {2},"
						+ " mainContent: {3}, newVersion: {4}, needBarcode: {5}, force: {6}, owner: {7},"
						+ " htmlSourceId: {8}, proceedBookmarks: {9}, proceedForms: {10}, content length: {11}",
				folderObjectId, templateObjectId, objectName, mainContent, newVersion, needBarcode, force, owner,
				htmlSourceId, proceedBookmarks, proceedForms, (content != null ? content.length() : 0));
		return str;
	}
}
