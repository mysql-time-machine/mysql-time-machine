import com.booking.validator.service.protocol.ValidationTaskDescription;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;

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

    }

}
