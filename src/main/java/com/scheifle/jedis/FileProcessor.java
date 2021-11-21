package com.scheifle.jedis;

import com.scheifle.jedis.redis.IRedisProcessor;

import java.io.*;

public class FileProcessor {

    private final IRedisProcessor redisProcessor;

    public FileProcessor(IRedisProcessor redisConnector){
        this.redisProcessor = redisConnector;
    }

    public void processFile(String path, FileType fileType) throws Exception {
        if(fileType.equals(FileType.POST))
            processPost(path);
        else if(fileType.equals(FileType.COMMENT))
            processComment(path);
        else
            throw new Exception("Unknown FileType");
    }

    private void processPost(String path) throws IOException {
        File file = new File(path);
        FileReader fileReader = new FileReader(file);
        BufferedReader br = new BufferedReader(fileReader);
        redisProcessor.start();
        String line;
        while((line = br.readLine())!=null){
            String[] splitLine = splitLine(line);
            redisProcessor.writePosts(splitLine);
        }
        fileReader.close();
        redisProcessor.sync();
    }

    private void processComment(String path) throws IOException {
        File file = new File(path);
        FileReader fileReader = new FileReader(file);
        BufferedReader br = new BufferedReader(fileReader);
        redisProcessor.start();
        String line;
        while((line = br.readLine())!=null){
            String[] splitLine = splitLine(line);
            redisProcessor.writeComments(splitLine);
        }
        fileReader.close();
        redisProcessor.sync();
    }

    private String[] splitLine(String line) {
        return line.split("\\|");
    }

}
