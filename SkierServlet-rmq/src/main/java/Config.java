import java.io.InputStream;
import java.util.Properties;

public class Config {
    private static final Properties properties = new Properties();

    static {
        try (InputStream input = Config.class.getClassLoader().getResourceAsStream("config.properties")) {
            properties.load(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String getRMQHost() {
        return properties.getProperty("rabbitmq.host");
    }

    public static int getRMQPort() {
        return Integer.parseInt(properties.getProperty("rabbitmq.port"));
    }

    public static String getRMQUsername() {
        return properties.getProperty("rabbitmq.username");
    }

    public static String getRMQPassword() {
        return properties.getProperty("rabbitmq.password");
    }
}