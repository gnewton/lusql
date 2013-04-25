package ca.gnewton.lusql.core;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Logger;


//This custom formatter formats parts of a log record to a single line
class LogFormatter extends Formatter {
  // This method is called for every log records
  public String format(LogRecord rec) {
    StringBuilder sb = new StringBuilder(1000);
    
    return rec.getLevel() 
	    + ": "
	    + calcDate(rec.getMillis())
	    + ": "
	    + rec.getSourceClassName() 
	    + "."
	    + rec.getSourceMethodName() 
	    + " "
	    + formatMessage(rec);
    
    /*
    if (rec.getLevel().intValue() >= Level.WARNING.intValue()) {
      buf.append("<b>");
      buf.append(rec.getLevel());
      buf.append("</b>");
    } else {
      buf.append(rec.getLevel());
    }
    buf.append("</td>");
    buf.append("<td>");
    buf.append(calcDate(rec.getMillis()));
    buf.append(' ');
    buf.append(formatMessage(rec));
    buf.append('\n');
    buf.append("<td>");
    buf.append("</tr>\n");
    return buf.toString();
    */
  }

  private String calcDate(long millisecs) {
    SimpleDateFormat date_format = new SimpleDateFormat("MMM dd,yyyy HH:mm");
    Date resultdate = new Date(millisecs);
    return date_format.format(resultdate);
  }

	public static void initLogger(final Logger log)
	{
		log.setLevel(Level.INFO);
		//log.setLevel(Level.CONFIG);
		//log.setLevel(Level.ALL);
		//Handler ch = new ConsoleHandler();
		//ch.setFormatter(new LogFormatter());
		//log.addHandler(ch);

	}
	

} 
