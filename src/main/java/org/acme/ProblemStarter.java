package org.acme;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import io.smallrye.mutiny.Multi;

import java.time.Duration;
import java.util.concurrent.Executors;

import static java.lang.Boolean.TRUE;

@QuarkusMain
public class ProblemStarter implements QuarkusApplication {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static JsonNode createProblematicJson() {
        try {
            JsonNode baseObject = mapper.readTree("{\"grouping_reason\":\"WHATEVER\"}");

            JsonNode otherObject = mapper.readTree("{}");

            ArrayNode arrayNode = mapper.createArrayNode();

            for (int i = 0; i < 11; i++) {
                arrayNode.add(baseObject.deepCopy()); // deepCopy to avoid reference sharing
            }
            //!!!!!!!!!!!!!!!!!!!!!!!!!! IF 120 replaced with 119 - IT WORKS
            for (int i = 0; i < 120; i++) {
                arrayNode.add(otherObject.deepCopy());
            }

            return arrayNode;
        }
        catch (Exception e){
            System.out.println(e.getMessage());
        }

        return null;
    }

    public Multi<Multi<JsonNode>> transform() {

        var problematicJson = createProblematicJson();

        return Multi.createFrom().item(Multi.createFrom().iterable(problematicJson).collect().asList())
                .onItem().transform(a ->
                        a.onItem().transformToMulti(Multi.createFrom()::iterable)
                                .log().group()
                                .by(market -> market.has("grouping_reason"))
                                .onItem()
                                //NOT EVEN MERGE WORKS - STRICTLY THE SIZE OF OTHER GROUP
                                .transformToMultiAndMerge(
                                //.transformToMultiAndConcatenate(
                                        group -> {
                                            return TRUE.equals(group.key())
                                                    ? Multi.createFrom().item((JsonNode)mapper.createArrayNode())
                                                    : Multi.createFrom().item((JsonNode)mapper.createArrayNode());
                                        })
                                //NO MATTER THE DURATION - IT JUST HANGS
                                .ifNoItem().after(Duration.ofSeconds(10)).fail().onFailure().invoke(throwable -> {
                                    System.out.println("❌ Failure: " + throwable);
                                    throwable.printStackTrace();
                                }) // Add timeout
                                .onCompletion()
                                .invoke(() -> System.out.println("Transformation completed"))
                );
    }

    @Override
    public int run(String... args) throws Exception {

        transform()
                .flatMap(x -> x)
                .runSubscriptionOn(Executors.newSingleThreadExecutor())
                .subscribe().with(
                        response -> System.out.println("Received: " + response),
                        failure -> System.err.println("Failed: " + failure)
                );

        Thread.sleep(200_000);

        return 0;
    }
}
