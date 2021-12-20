package br.com.senior.seniorx.integration.state;

public interface IntegrationState {

    default void put() {
        put(null);
    }

    default void put(String state) {
        put(state, null);
    }

    void put(String state, String stateMessage);

    <T> T get(Class<T> dataClass);

    default void update(String state) {
        update(state, null);
    }

    void update(String state, String stateMessage);

    void delete();

}
