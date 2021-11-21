package com.scheifle.jedis.models;

public class User {

    private long user_id;
    private String user;

    public User(long user_id, String user) {
        this.user_id = user_id;
        this.user = user;
    }

    @Override
    public String toString() {
        return "User{" +
                "user_id=" + user_id +
                ", user='" + user + '\'' +
                '}';
    }
}
