import org.apache.hadoop.util.ToolRunner;

public class CalculatorTest {
    public static void main(String[] args) throws Exception {

        String[] inputargs = new String[1];
        inputargs[0] = "test.txt";

        ToolRunner.run(new Calculator(), inputargs);
    }
}
