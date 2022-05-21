package com.example.springintegrationdemo;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.integration.config.EnableIntegration;

import com.example.springintegrationdemo.service.kafka.KafkaIntegrationService;
import lombok.extern.slf4j.Slf4j;

@EnableIntegration
@SpringBootApplication
@EnableConfigurationProperties
@Slf4j
public class SpringIntegrationDemoApplication {

	public static void main(String[] args) {
		ConfigurableApplicationContext context = new SpringApplicationBuilder(SpringIntegrationDemoApplication.class)
			.web(WebApplicationType.NONE)
			.run(args);

		context.getBean(KafkaIntegrationService.class).runDemo(context);
		context.close();
	}
}
