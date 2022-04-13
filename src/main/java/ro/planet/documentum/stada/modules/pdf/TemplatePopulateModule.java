package ro.planet.documentum.stada.modules.pdf;

import com.documentum.fc.client.DfSingleDocbaseModule;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfLogger;

import framework.ru.documentum.utils.SessionHelper;

/**
 * Преобразование шаблона word. Вставка значений атрибутов в документ.
 */
public class TemplatePopulateModule extends DfSingleDocbaseModule {

	public String execute(String folderObjectId, String templateObjectId, String objectName) {
		IDfSession session = null;
		String returnId = null;
		try {
			session = getSessionManager().newSession(getDocbaseName());
			debug("template population started");
			ITemplateSource source = new DefaultTemplateSource(session);
			TemplatePopulateAttrs attrs = new TemplatePopulateAttrs();
			attrs.setFolderObjectId(folderObjectId);
			attrs.setTemplateObjectId(templateObjectId);
			attrs.setObjectName(objectName);
			returnId = new TemplatePopulateHelper().executeWithSession(source, attrs);
			debug("template population finished");
		} catch (DfException dfex) {
			dfex.printStackTrace();
			error("DfException occured while populating template id: {0}", dfex, templateObjectId);
			// throw new RuntimeException("Template population failed: " +
			// templateObjectId);
		} finally {
			if (session != null)
				releaseSession(session);
		}
		return returnId;
	}

	public String executeEx(TemplatePopulateAttrs attrs) {
		IDfSession session = null;
		String returnId = null;
		try {
			session = getSessionManager().newSession(getDocbaseName());
			debug("template population started");
			ITemplateSource source = new DefaultTemplateSource(session);
			returnId = new TemplatePopulateHelper().executeWithSession(source, attrs);
			debug("template population finished");
		} catch (DfException dfex) {
			dfex.printStackTrace();
			error("DfException occured while populating template id: {0}", dfex, attrs.getTemplateObjectId());
			// throw new RuntimeException("Template population failed: " +
			// attrs.getTemplateObjectId());
		} finally {
			if (session != null)
				releaseSession(session);
		}
		return returnId;
	}

	public HTMLExtractResult extractHtml(HTMLExtractAttrs attrs) {
		IDfSession session = null;
		String returnId = null;
		try {
			session = getSessionManager().newSession(getDocbaseName());
			debug("template population started");
			HTMLExtractHelper helper = new HTMLExtractHelper(session);
			returnId = helper.execute(attrs.getObjectId());
			debug("template population finished");
		} catch (Throwable ex) {
			ex.printStackTrace();
			error("DfException occured while html extraction: {0}", ex, attrs.getObjectId());
			// throw new RuntimeException("HTML extraction failed: " +
			// attrs.getObjectId());
		} finally {
			if (session != null)
				releaseSession(session);
		}
		HTMLExtractResult result = new HTMLExtractResult();
		result.setResult(returnId);
		return result;
	}

	protected void debug(String message, Object... params) {
		// String string = MessageFormat.format(message, params);
		DfLogger.debug(this, message, params, null);
	}

	protected void error(String message, Throwable tr, Object... params) {
		// String string = MessageFormat.format(message, params);
		DfLogger.error(this, message, params, tr);

		if (tr != null) {
			try {
				tr.printStackTrace();
			} catch (Throwable ex) {
				debug("Cannot print stack trace");
			}
		}
	}

	public static void main(String[] args) {
		String docbase;
		String userName;
		String ticket;
		try {
			docbase = args[0];
			userName = args[1];
			ticket = args[2];
		} catch (Throwable tr) {
			System.out.println("Usage: <DOCBASE> <USER> <TICKET>");
			return;
		}
		try {
			SessionHelper sessionHelper = new SessionHelper((IDfSession) null);
			IDfSession dfSession = sessionHelper.getUserSession(userName, ticket, docbase);
			try {
				TemplatePopulateAttrs attrs = new TemplatePopulateAttrs();
				attrs.setForce("true");
				// attrs.setHtmlSourceId("09000386802fd55c");
				attrs.setObjectName("-");
				attrs.setTemplateObjectId("0900038680f759d4");
				attrs.setFolderObjectId("0b00038680f7598f");
				// attrs.setOwner("Сорока Екатерина Евгеньевна");
				attrs.setOwner("Vecherov_u2");
				// attrs.setNeedBarcode("false");
				// attrs.setProceedForms("false");
				// attrs.setProceedBookmarks("true");
				// attrs.setNewVersion("true");
				attrs.setContent("<div><span style=\"font-family:courier new;\">seyjgvrkuwergntuven5w45784nv5t9v4w5ntv94y59tnv4598yt3v05m8u3v8045yv934n59vy3459t8c347n5n87c4587cg4nc86gvn38645c6ebg8v2g458nc458gc84ngrtx8werh7n8cwer8hgc</span></div><div><span style=\"font-family:courier new;\">8732t4v58b2t54cn87245872c4587ngcwe87rfcq8nergfx87ncergn872457mch24578v2459nytv294597245tn947t9ncy49ntcw49n5ct4w95ycn9wytnc9w45y9c2n4y59tnc459nhc9enh5cw9mh59ncweh59xhec59nghe59tmvw45m9hvw49nh5tvw9mehtv9wnehv9whetvw</span></div><div><span style=\"font-family:courier new;\"><br/></span></div><div><span style=\"font-family:courier new;\">8723м4е28734пт843ес782упст93кр9тс23рп9ср2ук9пртс94рпк9ьчрукп9рсцу9ктспцу9крпсцт9р45ь9ср45птс495ьрпс97упчиупцтс457есцт74рем9ур5е9мр59црм459трц59ьмрыу59рыу9рт5пмы9ру59ьцру59трму597ру975рмцу975ремц94тр5м9цр9мтцр59прцм</span></div><div><span style=\"font-family:courier new;\">ловапмывтгркпмтшц45нмец4958ер2м9745м9т254р9м24р5т9емр249т5рем294рем93р45н9рмц459нтрм34597рмц59ь4рс97ьу5реьч7с5уь7серц4ь975рцм74р5м94р59мь4р5ьм9ыурь5е9ыру5м9ьрыкуьмрькыь9рнм9ну4рнм9ьуцрк5нмь9руцк59ьрмц95ьрмц49ь5рнмц9ьр45мц</span></div>");
				ITemplateSource source = new DefaultTemplateSource(dfSession);
				String returnId = new TemplatePopulateHelper().executeWithSession(source, attrs);
			} finally {
				sessionHelper.release();
			}
		} catch (Throwable tr) {
			tr.printStackTrace();
		}
	}
}
