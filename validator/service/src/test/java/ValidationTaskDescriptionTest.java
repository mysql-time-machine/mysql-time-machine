import com.booking.validator.service.protocol.DataPointerDescription;
import com.booking.validator.service.protocol.ValidationTaskDescription;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by psalimov on 9/13/16.
 */
public class ValidationTaskDescriptionTest {

    @Test
    public void serializationTest() throws IOException {

        String serialized = "{ \"source\" : { \"storage\" : { \"type\" : \"TypeS\", \"table\" : \"TableS\" }, \"key\" : { \"id\" : \"KeyS\" } },  \"target\" : { \"storage\" : { \"type\" : \"TypeT\", \"table\" : \"TableT\" }, \"key\" : { \"id\" : \"KeyT\" } } }";

        ObjectMapper mapper = new ObjectMapper();
        ValidationTaskDescription d = mapper.readValue(serialized, ValidationTaskDescription.class);

        assertEquals("TypeS", d.getSource().getStorageType());
        assertEquals("KeyT", d.getTarget().getKey().get("id"));

        d = mapper.readValue(serialized.getBytes(Charset.defaultCharset()), ValidationTaskDescription.class);

        assertEquals("TypeS", d.getSource().getStorageType());
        assertEquals("KeyT", d.getTarget().getKey().get("id"));

    }

    @Test
    public void serializationDeserializationTest() throws IOException{

        ObjectMapper mapper = new ObjectMapper();

        Map<String, String> constStorageDescription = new HashMap<>();
        constStorageDescription.put("type", "const");


        Map<String, String> constKeyDescription = new HashMap<>();
        constKeyDescription.put("value", "{ \"a\": 1, \"b\" : 2 }");

        DataPointerDescription source = new DataPointerDescription(constStorageDescription,constKeyDescription);
        ValidationTaskDescription task = new ValidationTaskDescription(source,source);


        String s = mapper.writeValueAsString(task);

        ValidationTaskDescription task2 = mapper.readValue(s, ValidationTaskDescription.class);

        assertEquals(task,task2);

    }


    @Test
    public void deserializeMap() throws IOException {

        String serialized = "{ \"k1\" : \"v1\", \"k2\" : \"v2\" }";

        ObjectMapper mapper = new ObjectMapper();
        Map<String,String> map = mapper.readValue(serialized, Map.class);

        assertEquals("v2", map.get("k2"));
    }

}
