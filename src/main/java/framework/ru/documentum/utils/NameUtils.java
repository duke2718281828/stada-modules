package framework.ru.documentum.utils;

public class NameUtils {
	
	public static String english = "abcdefghijklmnopqrstuvwxyz";

	/**
     * Метод возвращает дательный падеж для фамилии
     * @return
     */
    public static String getDativCasusSurname(String name, String gender){
    	if(name.isEmpty()){
    		return "";
    	}
    	
    	String firstLetter = name.substring(0, 1).toLowerCase();
    	if(english.contains(firstLetter)){
    		return name;
    	}
    	
    	if("male".equals(gender)){
    		if(name.endsWith("ов") || name.endsWith("ев") ||
    		   name.endsWith("ин") || name.endsWith("ын")) {   
    			return name + "у";
    		}
    		
    		if(name.endsWith("ий") || name.endsWith("ой") ) {   
    	    	return name.substring(0, name.length()-2) + "ому";
    	    }
    	}
    	
    	if("female".equals(gender)){
    		if(name.endsWith("ова") || name.endsWith("ева") ||
    		   name.endsWith("ина") || name.endsWith("ына")) {   
    			return name.substring(0, name.length()-1) + "ой";
    		}
    		
    		if(name.endsWith("кая"))  {   
    			return name.substring(0, name.length()-2) + "ой";
    	    }
    	}
    	
    	return name;
    }
    
    /**
     * Метод возвращает дательный падеж для имени
     * @return
     */
    public static String getDativCasusName(String name, String gender){
    	
    	if(name.isEmpty()){
    		return "";
    	}
    	
    	if(name.length() == 1 || (name.length() == 2 && name.endsWith("."))){
    		return name;
    	}
    	
    	String consonants = "бвгджзклмнпрстфхцчшщ";
      	String vowels = "аяоеёуюыиэ";
    	
    	String lastLetter = name.substring(name.length()-1);
    	String preLastLetter = name.substring(name.length()-2, name.length()-1);
    	
    	String firstLetter = name.substring(0, 1).toLowerCase();
    	if(english.contains(firstLetter)){
    		return name;
    	}
	
    	    	
    	if("male".equals(gender)){
    		if(name.equals("Павел")){
    			return "Павлу";
    		}
    		
    		if(consonants.contains(lastLetter)){
    			return name + "у";
    		} 
    		
    		if(name.endsWith("й") || name.endsWith("ь")){ // Алексей, Игорь
    			return name.substring(0, name.length()-1) + "ю";
    		}
    		
    		if(name.endsWith("ия")) { // Иеремия  
    			return name.substring(0, name.length()-1) + "и";
    		}
    		
    		// Николя, Педро, Франсуа 
    		if(name.equals("Николя") || "оеёуюыиэ".contains(lastLetter) || ("аяоёуюыэ".contains(preLastLetter) && vowels.contains(lastLetter))){
    			return name;
    		}
    		
    		// Илья, Абдулла 
    		if(name.endsWith("а") || name.endsWith("я")){
        		return name.substring(0, name.length()-1) + "е";
        	}
    		
    	} else if("female".equals(gender)){
    		if(name.endsWith("ия")) { // Мария   
    			return name.substring(0, name.length()-1) + "и";
    		}
    		
    		// Ольга, Валерия
    		if(name.endsWith("а") || name.endsWith("я")){
        		return name.substring(0, name.length()-1) + "е";
        	}
    		
    		if(name.endsWith("ь")){ // Любовь
    			return name.substring(0, name.length()-1) + "и";
    		}
    		
    		if("оеёуюыиэ".contains(lastLetter) || consonants.contains(lastLetter)) { // Алсу, Гретхен
    			return name;
    		}
    	}
    	
    	return name;
    }
    
    /**
     * Метод возвращает дательный падеж для отчества
     * @return
     */
    public static String getDativCasusMiddlename(String name, String gender){
    	if(name.isEmpty()) {
    		return "";
    	}
    	
    	if(name.length() == 1 || (name.length() == 2 && name.endsWith("."))){
    		return name;
    	}
    	
    	String firstLetter = name.substring(0, 1).toLowerCase();
    	if(english.contains(firstLetter)){
    		return name;
    	}
    	
    	if("male".equals(gender)){
    		return name + "у";
    	}
    	
    	if("female".equals(gender)){
    		return name.substring(0, name.length()-1) + "е";
    	}
    	
    	return name;
    }
    
    /**
     * Метод определяет пол по фамилии, имени и отчеству
     * @param surname
     * @param middlename
     * @return
     */
    public static String guessGender(String surname, String name, String middlename){
    	if(middlename != null && !middlename.isEmpty()){
    		if(middlename.endsWith("ович") || middlename.endsWith("евич") || middlename.endsWith("ич")){
    			return "male";
    		}
    		if(middlename.endsWith("овна") || middlename.endsWith("евна") || middlename.endsWith("ична")){
    			return "female";
    		}
    	}
    	
    	if(surname.endsWith("ов") || surname.endsWith("ев") ||
    	   surname.endsWith("ин") || surname.endsWith("ын") || 
    	   surname.endsWith("ий") || surname.endsWith("ой"))  {   
     	    	return "male";
     	}
    	
    	if(surname.endsWith("ова") || surname.endsWith("ева") ||
    	   surname.endsWith("ина") || surname.endsWith("ына") || 
    	   surname.endsWith("кая"))  {   
    	     	return "female";
    	}
    	

		if(name.endsWith("й") || name.endsWith("ь")){ 
			return "male";
		}
		
		if(name.endsWith("ия")) { 
			return "female";
		}
    	
    	return "male";
    }
}
