package edu.whpu;

import com.jd.platform.hotkey.client.ClientStarter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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

    @Value("${hotkey.app-name}")
    private String appName;

    @Value("${hotkey.etcd-server}")
    private String etcdServer;

    @Value("${hotkey.caffeine-size}")
    private int caffeineSize;

    @Value("${hotkey.push-period}")
    private Long pushPeriod;

    @PostConstruct
    public void initCache(){
        ClientStarter.Builder builder = new ClientStarter.Builder();
        ClientStarter starter = builder.setAppName(appName)
                .setEtcdServer(etcdServer)
                .setCaffeineSize(caffeineSize)
                .setPushPeriod(pushPeriod)
                .build();
        starter.startPipeline();
    }

}
