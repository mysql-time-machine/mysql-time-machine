import com.booking.validator.service.ValidatorConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

/**
 * Created by psalimov on 9/19/16.
 */
public class ValidatorConfigurationTest {

    @Test
    public void deserializationTest() throws IOException {

        String serialized = "data_sources:\n"+
                "    - name: 'a'\n" +
                "      type: 'aT'\n"+
                "      configuration:\n"+
                "         akey1 : av1\n"+
                "         akey2 : av2\n"+
                "    - name: 'b'\n" +
                "      type: 'bT'\n"+
                "      configuration:\n"+
                "         akey1 : bv1\n"+
                "         akey2 : bv2\n";

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        ValidatorConfiguration conf = mapper.readValue(serialized, ValidatorConfiguration.class);

        for (ValidatorConfiguration.DataSource dp : conf.getDataSources()){
            System.out.println("name: "+ dp.getName());
            System.out.println("type: "+ dp.getType());

            for (Map.Entry<String,String> e : dp.getConfiguration().entrySet()){
                System.out.println("conf: "+e.getKey()+" - "+e.getValue());
            }
        }

    }

}
