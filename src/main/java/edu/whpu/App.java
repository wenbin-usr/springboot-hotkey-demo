package edu.whpu;

import com.jd.platform.hotkey.client.ClientStarter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;

/**
 * Hello world!
 *
 */
@SpringBootApplication
public class App {

    public static void main( String[] args ) {
        SpringApplication.run(App.class,args);
    }

    @PostConstruct
    public void initCache(){
        ClientStarter.Builder builder = new ClientStarter.Builder();
        ClientStarter starter = builder.setAppName("")
                .setEtcdServer("")
                .setCaffeineSize(12)
                .setPushPeriod(10000L)
                .build();
        starter.startPipeline();
    }
}
