package ro.planet.documentum.stada.modules.pdf;

public class FieldSpec {
    public String prefix;
    public String data;
    public String suffix;

    public FieldSpec(String prefix, String data, String suffix) {
	super();
	this.prefix = prefix;
	this.data = data;
	this.suffix = suffix;
    }

    @Override
    public String toString() {
	return prefix + data + suffix;
    }

    public boolean parsed() {
	return prefix.length() > 0;
    }

    public boolean isDocProperty() {
	return prefix.indexOf("DOCPROPERTY") > -1;
    }

    /**
     * Field в Word имеет вид <br>
     * 
     * MERGEFIELD
     * bd_info_list[object_name=Info%20list0].bd_informed#*.dss_perf_status \*
     * MERGEFORMAT
     * 
     * Т. е. начинается на слово DOCPROPERTY или MERGEFIELD и заканчивается на
     * "\* MERGEFORMAT". Для MERGEFIELD может не иметь окончания.
     * 
     * <br>
     * Данный метол разделяет текст на 3 части, префикс, инструкция и суффикс.
     * 
     * @param value
     * @return
     */
    public static FieldSpec parseFieldSpec(String value) {
	String prevValue = value;
	value = value.trim();
	String[] prefixes = new String[] { "DOCPROPERTY", "MERGEFIELD" };
	String prefix = "";
	boolean hasPrefix = false;
	for (String str : prefixes) {
	    if (value.startsWith(str)) {
		prefix = str;
		value = value.substring(str.length());
		hasPrefix = true;
		break;
	    }
	}
	String suffix = "\\* MERGEFORMAT";
	boolean hasSuffix = value.endsWith(suffix);
	if (hasSuffix) {
	    value = value.substring(0, value.length() - suffix.length());
	} else {
	    // MERGEFIELD can be without suffix, DOCPROPETY without suffix not
	    // supported.
	    if (prefix.indexOf("DOCPROPERTY") > -1) {
		hasPrefix = false;
	    }
	}

	if (!(hasPrefix)) {
	    return new FieldSpec("", prevValue, "");
	}
	if (!(hasSuffix)) {
	    return new FieldSpec(" " + prefix + "  ", value.trim(), " ");
	}
	value = value.replace(" ", "");
	return new FieldSpec(" " + prefix + "  ", value.trim(), "  " + suffix + " ");
    }

}
