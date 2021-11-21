package com.scheifle.jedis.redis;

import com.scheifle.jedis.models.Comment;
import com.scheifle.jedis.models.Post;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.*;

public class RedisProcessor implements IRedisProcessor {

    private final String commentprefix = "comment#";
    private final String postprefix = "post#";
    private final String userprefix = "user#";
    private final String numberCommentsSuffix = "#numberComments";
    private Jedis jedis;
    private Pipeline pipeline;

    private int max = 50_000;
    private int counter = 0;
    private long currentMax = 0;

    @Override
    public void writePosts(String[] dataset) {
        String post_id = postprefix + dataset[1];
        Map<String, String> data = new HashMap<>();
        data.put("ts", dataset[0]);
        data.put("post_id", dataset[1]);
        data.put("user_id", dataset[2]);
        data.put("post", dataset[3]);
        data.put("user", dataset[4]);

        writeUsersIfNotExists(dataset[2], dataset[4]);
        pipeline.set(post_id + numberCommentsSuffix, "0");
        pipeline.hmset(post_id, data);

        syncRegularly("posts");
    }

    @Override
    public void writeComments(String[] dataset) {
        String comment_id = commentprefix + dataset[1];
        String post_id;
        Map<String, String> update_post_data = new HashMap<>();
        Map<String, String> data = new HashMap<>();

        data.put("ts", dataset[0]);
        data.put("comment", dataset[3]);
        data.put("user", dataset[4]);
        data.put("comment_id", dataset[1]);
        data.put("user_id", dataset[2]);

        writeUsersIfNotExists(dataset[2], dataset[4]);

        if (!dataset[5].isBlank()) {
            trimStringAtLength(dataset[5], 255);
            Response<List<String>> test = pipeline.hmget(commentprefix + dataset[5], "post_commented");

            data.put("comment_replied", dataset[5]);
            sync();
            String post_commented_value = test.get().get(0);
            start();

            post_id = postprefix + post_commented_value;
            data.put("post_commented", post_commented_value);
        } else {
            trimStringAtLength(dataset[6], 255);
            post_id = postprefix + dataset[6];
            data.put("post_commented", dataset[6]);
        }
        update_post_data.put(comment_id, "");
        pipeline.hmset(post_id, update_post_data);
        pipeline.incr(post_id + numberCommentsSuffix);
        pipeline.hmset(comment_id, data);
        syncRegularly("comments");
    }

    @Override
    public void sync(){
        pipeline.sync();
    }

    @Override
    public String userNameByID(long userID) {
        return jedis.hgetAll(userprefix + userID).get("user");
    }

    @Override
    public Post postByID(long postID) {
        Map<String, String> fields = jedis.hgetAll(postprefix + postID);
        return new Post(fields.get("ts"), Long.parseLong(fields.get("post_id")), Long.parseLong(fields.get("user_id")), fields.get("post"), fields.get("user"));
    }

    @Override
    public Comment commentByID(long commentID) {
        Map<String, String> fields = jedis.hgetAll(commentprefix + commentID);

        if (fields.containsKey("comment_replied")) {
            return new Comment(fields.get("ts"), Long.parseLong(fields.get("comment_id")), fields.get("comment"), fields.get("user_id"), fields.get("user"), Long.parseLong(fields.get("post_commented")), Long.parseLong(fields.get("comment_replied")));
        } else {
            return new Comment(fields.get("ts"), Long.parseLong(fields.get("comment_id")), fields.get("comment"), fields.get("user_id"), fields.get("user"), Long.parseLong(fields.get("post_commented")), -1L);
        }
    }

    @Override
    public List<Comment> commentsByPostID(long postID) {
        Map<String, String> map = jedis.hgetAll(postprefix + postID);
        List<Comment> commentList = new ArrayList<>();

        for (String x : map.keySet()) {
            if (x.contains(commentprefix)) {
                Map<String, String> fields = jedis.hgetAll(x);
                if (fields.containsKey("comment_replied")) {
                    commentList.add(new Comment(fields.get("ts"), Long.parseLong(fields.get("comment_id")), fields.get("comment"), fields.get("user_id"), fields.get("user"), Long.parseLong(fields.get("post_commented")), Long.parseLong(fields.get("comment_replied"))));
                } else {
                    commentList.add(new Comment(fields.get("ts"), Long.parseLong(fields.get("comment_id")), fields.get("comment"), fields.get("user_id"), fields.get("user"), Long.parseLong(fields.get("post_commented")), -1L));
                }
            }
        }
        return commentList;
    }

    @Override
    public Map<Post, Integer> mostCommentedPosts() {
        Map<Post, Integer> allPostsWithMaxComments = new HashMap<>();
        Set<String> numberCommentsString= jedis.keys(postprefix + "*" + numberCommentsSuffix);

        for(String data_key : numberCommentsString){
            String[] segments = splitLine(data_key);
            String data_string = jedis.get(data_key);

            long data_long = Long.parseLong(data_string);
            long post_id = Long.parseLong(segments[1]);

            int numberComments = Integer.parseInt(jedis.get(postprefix + post_id + numberCommentsSuffix));

            if(data_long > currentMax){
                currentMax = data_long;
                allPostsWithMaxComments.clear();
                allPostsWithMaxComments.put(postByID(post_id), numberComments);
                System.out.println("new currentMax = " + currentMax);
            }else if(data_long==currentMax){
                allPostsWithMaxComments.put(postByID(post_id), numberComments);
            }
        }
        return allPostsWithMaxComments;
    }

    @Override
    public void start() {
        pipeline = jedis.pipelined();
    }

    @Override
    public void open(final String host, final int port) {
        this.jedis = new Jedis(host, port);
    }

    @Override
    public void close() {
        jedis.close();
    }

    private void writeUsersIfNotExists(final String id, final String user) {
        String user_id = userprefix + id;
        pipeline.hsetnx(user_id, "user", user);
    }

    private void trimStringAtLength(String s, int maxLength) {
        s.substring(0, Math.min(s.length(), maxLength));
    }

    private String[] splitLine(String line) {
        return line.split("#");
    }

    private void syncRegularly(String fileType){
        if(counter < max){
            counter++;
        }else{
            sync();
            counter = 0;
            start();
            System.out.println("syc() " + fileType);
        }
    }
}
