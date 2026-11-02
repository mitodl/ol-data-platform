import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.openmetadata.service.util.DescriptionSanitizer;

/**
 * Prints OpenMetadata's DescriptionSanitizer.sanitize of each input, for ../description_sanitizer.json.
 *
 * <p>Reads one base64-encoded description per stdin line and writes the sanitized text, base64
 * encoded, one per line. Compile it beside DescriptionSanitizer.java from the OpenMetadata release
 * tag (openmetadata-service/src/main/java/org/openmetadata/service/util/) and the
 * owasp-java-html-sanitizer, java8-shim and java10-shim jars at the version that tag's pom.xml
 * pins:
 *
 * <pre>
 * javac -d build -cp "$JARS" DescriptionSanitizer.java SanitizeFixtures.java
 * java -cp "build:$JARS" SanitizeFixtures &lt; inputs.b64 &gt; outputs.b64
 * </pre>
 */
public final class SanitizeFixtures {
  public static void main(String[] args) throws Exception {
    BufferedReader in =
        new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
    for (String line = in.readLine(); line != null; line = in.readLine()) {
      String input = new String(Base64.getDecoder().decode(line), StandardCharsets.UTF_8);
      String out = DescriptionSanitizer.sanitize(input);
      System.out.println(Base64.getEncoder().encodeToString(out.getBytes(StandardCharsets.UTF_8)));
    }
  }
}
