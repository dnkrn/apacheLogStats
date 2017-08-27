package dnkrn.storm.apache.logstats.domain;

import java.time.LocalDateTime;

/**
 * Created by dinakaran on 4/21/17.
 */
public class AccessLogRecord {

    private String clientIpAddress;
    private String clientIdentity;          // typically `-`
    private String remoteUser;              // typically `-`
    private LocalDateTime dateTime;         // [day/month/year:hour:minute:second zone]
    private String request;                 // GET /foo ...`
    private String httpStatusCode;          // 200, 404, etc.
    private String bytesSent;               // may be `-`
    private String referer;                 // where the visitor came from
    private String userAgent;               // browser type



    public AccessLogRecord(String clientIpAddress, String clientIdentity, String remoteUser,
                           LocalDateTime dateTime, String request, String httpStatusCode,
                           String bytesSent, String referer, String userAgent) {
        this.clientIpAddress = clientIpAddress;
        this.clientIdentity = clientIdentity;
        this.remoteUser = remoteUser;
        this.dateTime = dateTime;
        this.request = request;
        this.httpStatusCode = httpStatusCode;
        this.bytesSent = bytesSent;
        this.referer = referer;
        this.userAgent = userAgent;
    }

    public String getClientIpAddress() {
        return clientIpAddress;
    }

    public String getClientIdentity() {
        return clientIdentity;
    }

    public String getRemoteUser() {
        return remoteUser;
    }

    public LocalDateTime getDateTime() {
        return dateTime;
    }

    public String getRequest() {
        return request;
    }

    public String getHttpStatusCode() {
        return httpStatusCode;
    }

    public String getBytesSent() {
        return bytesSent;
    }

    public String getReferer() {
        return referer;
    }

    public String getUserAgent() {
        return userAgent;
    }
}
