package com.scheifle.jedis;

import com.scheifle.jedis.models.Comment;
import com.scheifle.jedis.models.Post;
import com.scheifle.jedis.redis.RedisProcessor;

import java.util.List;
import java.util.Map;

public class Application {

    private static long startTime = -1;

    public static void main(String[] args) throws Exception {

        RedisProcessor redisConnector = new RedisProcessor();
        FileProcessor fileProcessor = new FileProcessor(redisConnector);

        redisConnector.open("127.0.0.1", 6379);

        //SCHREIBEN: 90 SEKUNDEN

        startTimer();
        System.out.println("STARTING WITH POSTS.DAT");
        fileProcessor.processFile("src/main/resources/posts.dat", FileType.POST);
        System.out.println("FINISHED POSTS.DAT");
        printTimerInSeconds();

        startTimer();
        System.out.println("STARTED WITH COMMENTS.DAT");
        fileProcessor.processFile("src/main/resources/comments.dat", FileType.COMMENT);
        System.out.println("FINISHED WITH COMMENTS.DAT");
        printTimerInSeconds();

        System.out.println(redisConnector.userNameByID(974));
        System.out.println(redisConnector.postByID(705185));
        System.out.println(redisConnector.commentByID(702760));
        List<Comment> commentList = redisConnector.commentsByPostID(103079215782L);

        for(Comment comment : commentList){
            System.out.println(comment);
        }

        //ca. 130sec.
        startTimer();
        Map<Post, Integer> postMap = redisConnector.mostCommentedPosts();
        printTimerInSeconds();

        for(Post post : postMap.keySet()){
            System.out.println(post + "| NumberComments: " + postMap.get(post));
        }
        System.out.println("SIZE: " + postMap.size());

        redisConnector.close();
    }

    private static void printTimerInSeconds() {
        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println("Done. Time: " + estimatedTime / 1000 +"sec.");
    }

    private static void startTimer() {
        startTime = System.currentTimeMillis();
    }
}