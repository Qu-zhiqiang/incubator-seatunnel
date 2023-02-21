package config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class FileSyncConfig {
    public static final Option<String> PASSWORD = Options.key("password")
        .stringType()
        .noDefaultValue()
        .withDescription("SFTP server password");
    public static final Option<String> USERNAME = Options.key("user")
        .stringType()
        .noDefaultValue()
        .withDescription("SFTP server username");
    public static final Option<String> HOST = Options.key("host")
        .stringType()
        .noDefaultValue()
        .withDescription("SFTP server host");
    public static final Option<Integer> PORT = Options.key("port")
        .intType()
        .noDefaultValue()
        .withDescription("SFTP server port");
    public static final Option<String> FILE_PATH = Options.key("file.path")
        .stringType()
        .noDefaultValue()
        .withDescription("file path");
    public static final Option<String> SERVER_TYPE = Options.key("server.type")
        .stringType()
        .noDefaultValue()
        .withDescription("file path");
}
