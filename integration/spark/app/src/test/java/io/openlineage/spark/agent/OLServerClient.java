/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.server.OpenLineage;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.mockserver.model.RequestDefinition;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.nio.file.Files.readAllBytes;
import static org.awaitility.Awaitility.await;
import static org.mockserver.model.HttpRequest.request;

@Slf4j
public class OLServerClient {
  private String uri;
  private ObjectMapper objectMapper = new ObjectMapper();
  OLServerClient(String uri) {
    uri = uri;
  }

  public void verifyEvents(String... eventFiles) {
    verifyEvents(Collections.emptyMap(), eventFiles);
  }

  /**
   * @param replacements map of string replacements within the json files. Allows injecting spark
   *     version into json files.
   * @param eventFiles
   */
  @SneakyThrows
  public void verifyEvents(Map<String, String> replacements, String... eventFiles) {
    Path eventFolder = Paths.get("integrations/container/");

    Arrays.stream(eventFiles)
      .map(eventFolder::resolve)
      .map(Path::toFile)
      .map(objectMapper::readTree)
      .map(objectMapper.)

    await()
      .atMost(Duration.ofSeconds(20))
      .untilAsserted(
        () ->
          mockServerClient.verify(
            Arrays.stream(eventFiles)
              .map(
                fileEvent ->
                  request()
                    .withPath("/api/v1/lineage")
                    .withBody(
                      readJson(eventFolder.resolve(fileEvent), replacements)))
              .collect(Collectors.toList())
              .toArray(new RequestDefinition[0])));
  }


  @SneakyThrows
  public Optional<OpenLineage.RunEvent> getEvent(String jobName) {
    final HttpPost httpPost = new HttpPost(getLineageUri());
    final List<NameValuePair> params = new ArrayList<NameValuePair>();
    params.add(new BasicNameValuePair("job_name", jobName));
    httpPost.setEntity(new UrlEncodedFormEntity(params));

    httpPost.setHeader("Accept", "application/json");
    httpPost.setHeader("Content-type", "application/json");

    try (CloseableHttpClient client = HttpClients.createDefault()) {
      try (CloseableHttpResponse response = client.execute(httpPost)) {
        log.error("GOT {}", response.getStatusLine().getStatusCode());
        if (response.getStatusLine().getStatusCode() >= 300) {
          return Optional.empty();
        }
        OpenLineage.RunEvent runEvent = objectMapper.readValue(response.getEntity().getContent(), OpenLineage.RunEvent.class);
        log.error("EVENT {}", objectMapper.writeValueAsString(runEvent));
        return Optional.of(runEvent);
      }
    }
  }


  @SneakyThrows
  private JsonNode readJson(Path path) {
    return objectMapper.readTree(path.toFile());
  }

  private String getLineageUri() {
    return uri + "/v1/api/lineage";
  }
}
