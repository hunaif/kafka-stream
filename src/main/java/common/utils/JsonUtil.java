package common.utils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.VisibilityChecker;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Created by srisri on 29/04/15.
 * Search System
 */
public class JsonUtil {
    private static JsonUtil ourInstance = new JsonUtil();
    private ObjectMapper mapper;
    private JsonNode defaultNode;

    private JsonUtil() {
        mapper = new ObjectMapper();
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        mapper.setVisibilityChecker(VisibilityChecker.Std.defaultInstance().withFieldVisibility(JsonAutoDetect.Visibility.ANY));
        defaultNode = new ObjectNode(JsonNodeFactory.instance);
    }

    public static JsonUtil getInstance() {
        return ourInstance;
    }

    public JsonNode getDefaultNode() {
        return defaultNode;
    }

    public ObjectMapper getMapper() {
        return mapper;
    }
}
