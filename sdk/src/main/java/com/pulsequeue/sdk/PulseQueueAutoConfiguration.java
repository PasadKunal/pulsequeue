package com.pulsequeue.sdk;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;

@Configuration
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnProperty(prefix = "pulsequeue", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(PulseQueueProperties.class)
public class PulseQueueAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public PulseQueueClient pulseQueueClient(
            PulseQueueProperties props,
            ObjectMapper objectMapper,
            @Value("${spring.application.name:unknown-service}") String appName
    ) {
        if (props.getEndpoint() == null || props.getEndpoint().isBlank()) {
            throw new IllegalStateException(
                "pulsequeue.endpoint must be set. Example: pulsequeue.endpoint=https://pulsequeue-f1e3.onrender.com"
            );
        }
        if (props.getServiceName() == null || props.getServiceName().isBlank()) {
            props.setServiceName(appName);
        }
        return new PulseQueueClient(props, objectMapper);
    }

    @Bean
    @ConditionalOnMissingBean
    public FilterRegistrationBean<PulseQueueFilter> pulseQueueFilter(
            PulseQueueClient client,
            PulseQueueProperties props
    ) {
        FilterRegistrationBean<PulseQueueFilter> registration = new FilterRegistrationBean<>();
        registration.setFilter(new PulseQueueFilter(client, props));
        registration.addUrlPatterns("/*");
        registration.setOrder(Ordered.LOWEST_PRECEDENCE - 10);
        registration.setName("pulseQueueFilter");
        return registration;
    }
}
