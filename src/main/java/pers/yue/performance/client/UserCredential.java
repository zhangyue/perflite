package pers.yue.performance.client;

public class UserCredential {
    private String pin;
    private String userId;
    private String accessKey;
    private String secretKey;

    public static final String UNDEFINED = "UNDEFINED";

    public UserCredential(String userPropertyString) {
        if(userPropertyString == null || userPropertyString.isEmpty()) {
            throw new RuntimeException("User property is null or empty.");
        }

        if(userPropertyString == UNDEFINED) {
            this.pin = UNDEFINED;
            this.userId = UNDEFINED;
            this.accessKey = UNDEFINED;
            this.secretKey = UNDEFINED;
            return;
        }

        String[] userProperties = userPropertyString.split(":");
        if(userProperties.length != 4) {
            throw new RuntimeException("Unexpected number of user properties, splited by \":\" - " + userPropertyString);
        }

        this.pin = userProperties[0];
        this.userId = userProperties[1];
        this.accessKey = userProperties[2];
        this.secretKey = userProperties[3];
    }

    public UserCredential(String pin, String userId, String accessKey, String secretKey) {
        this.pin = pin;
        this.userId = userId;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    public String getPin() {
        return pin;
    }

    public String getUserId() {
        return userId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String toString() {
        return getPin() + " " + getUserId() + " " + getAccessKey();
    }
}
