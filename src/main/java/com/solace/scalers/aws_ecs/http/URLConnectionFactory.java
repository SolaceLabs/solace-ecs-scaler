package com.solace.scalers.aws_ecs.http;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;

public interface URLConnectionFactory {
    HttpURLConnection createConnection(String urlString) throws IOException, URISyntaxException;
}
