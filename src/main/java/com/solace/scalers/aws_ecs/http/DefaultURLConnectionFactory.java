package com.solace.scalers.aws_ecs.http;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;

public class DefaultURLConnectionFactory implements URLConnectionFactory {
    @Override
    public HttpURLConnection createConnection(String urlString) throws IOException, URISyntaxException {
        return (HttpURLConnection) new URI(urlString).toURL().openConnection();
    }
}
