package dnkrn.storm.apache.logstats.services;

import dnkrn.storm.apache.logstats.domain.AccessLogRecord;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by dinakaran on 4/21/17.
 */
public class AccessLogParser {

    private String ddd = "\\d{1,3}" ;                     // at least 1 but not more than 3 times (possessive)
    private String ip = "(ddd\\.ddd\\.ddd\\.ddd)?";  // like `123.456.7.89`
    private String client = "(\\S+)";                 // '\S' is 'non-whitespace character'
    private String user = "(\\S+)";
    private String dateTime = "(\\[.+?\\])" ;             // like `[21/Jul/2009:02:48:13 -0700]`
    private String request = "\"(.*?)\"" ;                // any number of any character, reluctant
    private String status = "(\\d{3})";
    private String bytes = "(\\S+)" ;                   // this can be a "-"
    private String referer = "\"(.*?)\"";
    private String agent = "\"(.*?)\"";

    private String regex = ip +client +user +dateTime +request +status +bytes +referer +agent;
    private Pattern p = Pattern.compile(regex);



    public Optional<AccessLogRecord> parseRecord(String record){
        Matcher matcher = p.matcher(record);
        if (matcher.find()) {
            return Optional.ofNullable(buildAccessLogRecord(matcher));
        } else {
            return Optional.empty();
        }
    }


    private AccessLogRecord buildAccessLogRecord(Matcher matcher)  {
        return new AccessLogRecord(
                matcher.group(1),
                matcher.group(2),
                matcher.group(3),
                LocalDateTime.parse(matcher.group(4)),
                matcher.group(5),
                matcher.group(6),
                matcher.group(7),
                matcher.group(8),
                matcher.group(9));


    }
}
