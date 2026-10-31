package com.example.schemas;

import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import com.fasterxml.jackson.databind.*;
import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

/** Uses the real Registry REST API so the compatibility decisions are visible. */
@Component
public class RegistryClient {
    public static final String SUBJECT="sample.schemas.orders-value";
    @Value("${demo.registry-url}") private String base;
    private final ObjectMapper json;
    private final HttpClient http=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public RegistryClient(ObjectMapper json) {
        this.json=json;
    }
    // Call the actual Schema Registry API with a timeout instead of waiting forever.
    public HttpResponse<String> request(String method,String path,Object body) throws Exception {
        var builder=HttpRequest.newBuilder(URI.create(base+path)).timeout(Duration.ofSeconds(10)).header("Content-Type","application/vnd.schemaregistry.v1+json");
        builder.method(method,body==null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(json.writeValueAsString(body)));
        return http.send(builder.build(),HttpResponse.BodyHandlers.ofString());
    }
    // Read one small schema file from this project so its fields are easy to inspect.
    public Schema local(String version) throws Exception {
        if (!java.util.Set.of("v1","v2","bad").contains(version)) throw new IllegalArgumentException("Use v1, v2, or bad");
        try (var in=new ClassPathResource("schemas/"+version+".avsc").getInputStream()) {
            return new Schema.Parser().parse(new String(in.readAllBytes(),StandardCharsets.UTF_8));
        }
    }
    // Ask the Registry to check and save the schema using the chosen compatibility rule.
    public HttpResponse<String> register(String version) throws Exception {
        // Full-transitive mode checks both reading directions against all earlier schemas.
        var setting=request("PUT","/config/"+SUBJECT,Map.of("compatibility","FULL_TRANSITIVE"));
        if (setting.statusCode()!=200) throw new IllegalStateException(setting.body());
        // Call the actual Schema Registry API with a timeout instead of waiting forever.
        return request("POST","/subjects/"+SUBJECT+"/versions",Map.of("schema",local(version).toString()));
    }
    // Find the schema using its ID so a consumer knows how the message bytes were written.
    public Schema lookup(int id) throws Exception {
        var response=request("GET","/schemas/ids/"+id,null);
        if (response.statusCode()!=200) throw new IllegalStateException(response.body());
        return new Schema.Parser().parse(json.readTree(response.body()).path("schema").asText());
    }
    // Return the stored schema ID that we must put at the start of the Kafka value.
    public int registerId(String version) throws Exception {
        var response=register(version);
        if (response.statusCode()!=200) throw new IllegalStateException(response.body());
        return json.readTree(response.body()).path("id").asInt();
    }
}
