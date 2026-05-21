package edu.whpu;

import com.jd.platform.hotkey.client.callback.JdHotKeyStore;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Component
public class Cache {

    @Resource
    private RedisTemplate<String, String> redisTemplate;

    public String getFromRedis(String key) {
        return redisTemplate.opsForValue().get(key);
    }

    public String get(String key) {
        // 如果是热key，从本地Caffeine缓存中获取，有可能为null
        // 如果不是热key，则返回null，并且将key上报到探测集群进行数量探测。
        Object object = JdHotKeyStore.getValue(key);
        //如果已经缓存过了
        if (object != null) {
            System.out.println(key + " is hot key");
            return object.toString();
        } else {
            System.out.println(key + " not hot key");
            String value = getFromRedis(key);
            // 如果是热 key，该方法才会赋值，向本地Caffeine中写入缓存，非热 key，什么也不做
            JdHotKeyStore.smartSet(key, value);
            return value;
        }
    }

    public void set(String key, String value) {
        redisTemplate.opsForValue().set(key, value);
    }

    public void remove(String key) {
        JdHotKeyStore.remove(key);
    }

}
