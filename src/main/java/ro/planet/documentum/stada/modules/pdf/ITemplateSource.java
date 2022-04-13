package ro.planet.documentum.stada.modules.pdf;

import java.io.File;
import java.util.List;

import com.documentum.fc.client.IDfFolder;
import com.documentum.fc.client.IDfPersistentObject;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.client.IDfTypedObject;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.IDfId;

/**
 * 
 * Интерфейс для сохранения, извлечения объектов при формировании содержимого
 * word.
 */
public interface ITemplateSource {

    public IDfPersistentObject getObject(IDfId id) throws DfException;

    public IDfSysObject getTargetObject(IDfSysObject templateObject, IDfPersistentObject folderObject,
	    String objectName, TemplatePopulateAttrs attrs, boolean newVersion) throws DfException;

    public IDfSession getDfSession();

    /**
     * Сохранение объекта.
     * 
     * @param templateObject
     *            Шаблон.
     * @param newObject
     *            Новый объект, который возвращен методом getTargetObject.
     * @param tempFile
     *            Файл.
     * @param uid
     *            УИД.
     * @param owner
     *            Владелец объекта. Необязательное поле.
     * @throws DfException
     */
    public void saveTargetObject(IDfSysObject templateObject, IDfSysObject newObject, File tempFile, String uid,
	    String owner) throws DfException;

    public List<IDfPersistentObject> getChildren(IDfPersistentObject caseFolder, String relationName)
	    throws DfException;

    public IDfSysObject getTemplateObject(IDfId templateId) throws DfException;

    public static final String CURRENT_OBJECT_SIGN = "#";
    public static final String FETCH_RECURSIVE_SIGN = "*";
    public static final String FETCH_FIRST_SIGN = "!";
    public static final String FETCH_JOIN_SIGN = "@";
    public static final String CUSTOM_ATTRIBUTE_SIGN = "$";
}
