package ro.planet.documentum.stada.modules.pdf;

import static ro.planet.documentum.stada.modules.pdf.FieldSpecUtils.*;

/**
 * Парсит имя поля на компоненты.
 * 
 * Например, спецификация имеет вид <br>
 * DOCPROPERTY DMSxCP.dss_reg_number.dss_reg_number .
 * 
 * Имя поля имеет вид <br>
 * DMSxCP.dss_reg_number.dss_reg_number.
 * 
 * name = DMSxCP.dss_reg_number;
 * 
 * data = dss_reg_number.
 */

public class FieldName {

    private String source;

    private String type;

    private String name;

    private String data;

    private String warning;

    public String getName() {
	return name;
    }

    public String getData() {
	return data;
    }

    public String getWarning() {
	return warning;
    }

    public String getSource() {
	return source;
    }

    public String getType() {
	return type;
    }

    public FieldName(String param) {
	source = param;
	data = param;
	name = "";
	type = "";

	if (param.startsWith(DMS_PROP_RREFIX) || param.startsWith(DMS_PROP_RREFIX_UTIL)
		|| param.startsWith(PROP_RREFIX_UTIL)) {
	    String[] items = split(param, 3);
	    if (items.length != 3) {
		warning = "Invalid field specification";
		name = "";
		data = "";
		return;
	    }

	    data = items[2];
	    name = items[0] + "." + items[1];
	    type = items[0];
	}
    }
}
