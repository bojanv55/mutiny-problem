package org.acme;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import io.smallrye.mutiny.GroupedMulti;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Executors;

import static java.lang.Boolean.TRUE;

@QuarkusMain
public class ProblemStarter implements QuarkusApplication {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static List<JsonNode> createProblematicJson() {
        try {
            JsonNode baseObject = mapper.readTree("{\"grouping_reason\":\"WHATEVER\"}");

            JsonNode otherObject = mapper.readTree("{}");

            ArrayNode arrayNode = mapper.createArrayNode();

            for (int i = 0; i < 11; i++) {
                arrayNode.add(baseObject.deepCopy()); // deepCopy to avoid reference sharing
            }
            //!!!!!!!!!!!!!!!!!!!!!!!!!! >129 WORKS OK
            for (int i = 0; i < 130; i++) {
                arrayNode.add(otherObject.deepCopy());
            }

            return List.of(arrayNode);
        }
        catch (Exception e){
            System.out.println(e);
        }

        return null;
    }

    public Multi<Multi<JsonNode>> transform() {

        var str = createProblematicJson();

        Multi<Uni<List<JsonNode>>> outer = Multi.createFrom().emitter(emitter -> {
            for (JsonNode sublist : str) {
                Uni<List<JsonNode>> subMulti = Multi.createFrom().iterable(sublist).collect().asList();
                emitter.emit(subMulti);
            }
            emitter.complete();
        });

        return outer
                .onItem().transform(a ->
                        a.onItem().transformToMulti(Multi.createFrom()::iterable)
                                .log().group()
                                .by(market -> market.has("grouping_reason"))
                                .onItem()
                                .invoke(item -> System.out.println(item))
                                .onItem()
                                //IF CONCAT REPLACED WITH MERGE - IT WORKS
                                //.transformToMultiAndMerge(
                                .transformToMultiAndConcatenate(
                                        group -> {
                                            return TRUE.equals(group.key())
                                                    ? createSeparateMessageForEachVoidedMarket(group)
                                                    : updateMessageToHaveOnlySettledMarkets(group);
                                        })
                                //NO MATTER THE DURATION - IT JUST HANGS
                                .ifNoItem().after(Duration.ofSeconds(10)).fail().onFailure().invoke(throwable -> {
                                    System.out.println("❌ Failure: " + throwable);
                                    throwable.printStackTrace();
                                }) // Add timeout
                                .onCompletion()
                                .invoke(() -> System.out.println("Settlements transformation completed"))

                )

                ;
    }

    private Multi<JsonNode> updateMessageToHaveOnlySettledMarkets(GroupedMulti<Boolean, JsonNode> settledMarketsGroup) {
        return settledMarketsGroup
                .onCompletion().invoke(() -> System.out.println("Completed 2"))
                .collect()
                .asList()
                .onItem()
                .transformToMulti(
                        markets -> Multi.createFrom().item(new ObjectMapper().createObjectNode()));
    }

    private Multi<JsonNode> createSeparateMessageForEachVoidedMarket(GroupedMulti<Boolean, JsonNode> voidedMarketsGroup) {
        return voidedMarketsGroup
                .onCompletion().invoke(() -> System.out.println("Completed1"))
                .onItem()
                .transformToUniAndMerge(
                        market -> Uni.createFrom().item(new ObjectMapper().createObjectNode()))
                .collect()
                .asList()
                .onItem()
                .transformToMulti(Multi.createFrom()::iterable);
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
