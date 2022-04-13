package framework.ru.documentum.services;


public interface IServiceLogger {

    public void debug(String message, Object params[], Throwable t);
   
    public void error(String message, Object params[], Throwable t);
        
}
