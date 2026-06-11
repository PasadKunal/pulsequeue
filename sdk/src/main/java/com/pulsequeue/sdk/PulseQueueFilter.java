package com.pulsequeue.sdk;

import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;

public class PulseQueueFilter implements Filter {

    private final PulseQueueClient client;
    private final PulseQueueProperties props;

    public PulseQueueFilter(PulseQueueClient client, PulseQueueProperties props) {
        this.client = client;
        this.props = props;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        HttpServletRequest httpRequest = (HttpServletRequest) request;

        if (shouldSkip(httpRequest)) {
            chain.doFilter(request, response);
            return;
        }

        long start = System.currentTimeMillis();
        try {
            chain.doFilter(request, response);
        } finally {
            long latency = System.currentTimeMillis() - start;
            int status = ((HttpServletResponse) response).getStatus();
            client.record(props.getServiceName(), latency, status);
        }
    }

    private boolean shouldSkip(HttpServletRequest request) {
        String path = request.getRequestURI();
        return props.getExcludePaths().stream().anyMatch(path::startsWith);
    }
}
