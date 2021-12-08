package br.com.senior.seniorx.integration.state;

public interface IntegrationState {

    default void put() {
        put(null);
    }

    default void put(String state) {
        put(state, null);
    }

    void put(String state, String stateMessage);

    String get();

    void delete();

}
