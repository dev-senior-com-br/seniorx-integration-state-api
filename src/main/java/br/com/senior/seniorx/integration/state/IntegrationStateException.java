package br.com.senior.seniorx.integration.state;

public class IntegrationStateException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public IntegrationStateException(String message) {
        super(message);
    }

    public IntegrationStateException(Throwable cause) {
        super(cause);
    }

}
