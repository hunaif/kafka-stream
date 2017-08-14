package common.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by srisri on 15/05/15.
 * Search System
 */
public class MapperUtil {
    private static Logger log = Logger.getLogger(MapperUtil.class);

    private static ObjectMapper mapper = JsonUtil.getInstance().getMapper();
    private static JsonNode defaultNode = JsonUtil.getInstance().getDefaultNode();


    private MapperUtil() {
    }

    public static <T> T fromStream(InputStream stream, Class<T> valueType) {
        T request = null;
        try {
            //noinspection unchecked
            request = mapper.readValue(stream, valueType);
        } catch (IOException e) {
            log.error("Exception in mapping", e);
        }
        return request;
    }

    public static <T> T fromObject(Object fromValue, Class<T> valueType) {

        T request = null;
        try {
            request = mapper.convertValue(fromValue, valueType);
        } catch (Exception e) {
            log.error("Exception in mapping", e);
        }
        return request;
    }


    //todo in content return, ignore sending null valued fields :)
    public static <T> JsonNode getJsonNodeFromObject(T t) {
        JsonNode jsonNode = defaultNode;
        try {
            jsonNode = mapper.valueToTree(t);
        } catch (Exception e) {
            log.error("Exception in mapping to jsonNode", e);
        }
        return jsonNode;
    }

    public static JsonNode getJsonNodeFromString(String s) {
        JsonNode jsonNode = defaultNode;
        try {
            jsonNode = mapper.readTree(s);
        } catch (Exception e) {
            log.error("Exception in mapping to jsonNode from string", e);
        }
        return jsonNode;
    }


}
