package sna.data;

import net.sandrohc.jikan.Jikan;
import net.sandrohc.jikan.exception.JikanQueryException;
import net.sandrohc.jikan.model.anime.Anime;
import reactor.core.publisher.Flux;

public class Main {

    public static void main(String[] args) throws JikanQueryException {
        System.out.println("Hello World");
        Jikan jikan = new Jikan();

        long startTime = System.currentTimeMillis();

        Anime anime = jikan.query().anime().get(1)
                .execute()
                .block();

        Flux<Anime> top100 = Flux.range(1, 4)
                .concatMap(i -> {
                    System.out.println("Fetching page " + i);
                    try {
                        return jikan.query().anime().top().limit(25).page(i).execute();
                    } catch (JikanQueryException e) {
                        throw new RuntimeException(e);
                    }
                });

        top100.collectList().block().forEach(System.out::println);

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        System.out.println("Total time taken: " + duration + " milliseconds");
    }
}