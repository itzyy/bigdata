package cn.kepuchina.mr;

/**
 * Created by LENOVO on 2017/7/11.
 */
public class AccessLog {

    //设备ip
    private String appid;
    //ip
    private String ip;
    //macip
    private String macip;
    private String userid;
    //登陆类别
    private String loginType;
    //访问地址
    private String request;
    //访问状态
    private String status;
    //
    private String httpReferer;
    //访问浏览器
    private String userAgent;
    //访问时间
    private String time;
    private String datetime;


    private String method;
    private String path;
    private String http_version;

    public AccessLog(String appid, String macip, String userid, String loginType, String status, String httpReferer, String time) {
        this.appid = appid;
        this.macip = macip;
        this.userid = userid;
        this.loginType = loginType;
        this.status = status;
        this.httpReferer = httpReferer;
        this.time = time;
    }

    @Override
    public String toString() {
        return String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s",
                getAppid(),  getMacip(), getUserid(), getLoginType(), getStatus(), getHttpReferer(), getTime());
    }


    public String getAppid() {
        return appid;
    }

    public void setAppid(String appid) {
        this.appid = appid;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getMacip() {
        return macip;
    }

    public void setMacip(String macip) {
        this.macip = macip;
    }

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public String getLoginType() {
        return loginType;
    }

    public void setLoginType(String loginType) {
        this.loginType = loginType;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getHttp_version() {
        return http_version;
    }

    public void setHttp_version(String http_version) {
        this.http_version = http_version;
    }

    public String getDatetime() {
        return datetime;
    }

    public void setDatetime(String datetime) {
        this.datetime = datetime;
    }

    public String getHttpReferer() {
        return httpReferer;
    }

    public void setHttpReferer(String httpReferer) {
        this.httpReferer = httpReferer;
    }
}
