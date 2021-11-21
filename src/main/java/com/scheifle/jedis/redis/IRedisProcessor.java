package com.scheifle.jedis.redis;

import com.scheifle.jedis.models.Comment;
import com.scheifle.jedis.models.Post;

import java.util.List;
import java.util.Map;

public interface IRedisProcessor {
    String userNameByID(long userID);
    Post postByID(long postID);
    Comment commentByID(long commentID);
    List<Comment> commentsByPostID(long postID);
    Map<Post, Integer> mostCommentedPosts();

    void writePosts(String[] post);
    void writeComments(String[] comment);

    void open(final String host, final int port);
    void start();
    void sync();
    void close();

}
