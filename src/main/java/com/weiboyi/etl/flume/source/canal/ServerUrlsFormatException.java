package com.weiboyi.etl.flume.source.canal;


public class ServerUrlsFormatException extends Exception {
    private static final long serialVersionUID = 1L;

    public ServerUrlsFormatException() {
        super();
    }

    public ServerUrlsFormatException(String msg) {
        super(msg);
    }

    public ServerUrlsFormatException(String msg, Throwable throwable) {
        super(msg, throwable);
    }
}
