package sna.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import net.sandrohc.jikan.Jikan;
import net.sandrohc.jikan.Jikan.JikanBuilder;
import net.sandrohc.jikan.exception.JikanQueryException;
import net.sandrohc.jikan.model.anime.Anime;
import net.sandrohc.jikan.model.character.CharacterBasic;
import net.sandrohc.jikan.model.character.CharacterRole;
import net.sandrohc.jikan.model.common.Studio;
import reactor.core.publisher.Flux;

import java.io.BufferedWriter;
import java.io.FileWriter;

public class DataLoader {

    private Jikan jikan;

    public DataLoader() { jikan = new Jikan();}

    public static void main (String[] args) {  
        DataLoader loader = new DataLoader();
        //loader.loadStudios("./data/Anime_Studios.csv");
        //loader.loadProducers();
        loader.loadVoiceActors("./data/VoiceActors.csv");
	}

    private Flux<Anime> getRawData() {
        Flux<Anime> topAnime = Flux.range(1, 40)
        .concatMap(page -> {
            try {
                return jikan.query().anime().top().limit(25).page(page).execute();
            } catch (JikanQueryException e) {
                throw new RuntimeException(e);
            }
        });
        return topAnime;
    }

    public void loadStudios(String writerPath) {
        String[] header = {"rank", "anime", "type", "studios"};
        Map<String[], List<String>> data = new HashMap<>();
        CSVFormat format = CSVFormat.DEFAULT.builder().setHeader(header).build();

        System.out.println("Loading data into programm");
        Flux<Anime> topAnime = getRawData();
        System.out.println("Done");

        System.out.println("Format data for csv");
        topAnime.toStream().forEach(anime -> {
            String title = anime.getTitle();
            String rank = String.valueOf(anime.getRank());
            String type = anime.getType().getSearch();
            String[] rank_title = {rank, title, type};
            List<String> studios = anime.getStudios().stream().map(Studio::getName).toList();
            data.put(rank_title, studios);
        });
        System.out.println("Done");     

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(writerPath));
            final CSVPrinter printer = new CSVPrinter(writer, format)) {
            
            System.out.println("Writing data to csv");
            data.forEach((rank_title, studios) -> {
                studios.stream().forEach(studio -> {
                    try {
                        printer.printRecord(rank_title[0], rank_title[1],rank_title[2], studio);
                    } catch (IOException ioe) {
                        ioe.printStackTrace();
                    }
                });
            });

            printer.flush();  
            System.out.println("CSV file was created successfully.");

        } catch (IOException ioe) {
            System.out.println(ioe.getMessage());
        }

    }

    public void loadProducers() {
        JikanBuilder builder = new JikanBuilder();
        builder = builder.baseUrl("https://api.jikan.moe/v4/top/anime?type=tv");
        Jikan jikan = new Jikan(builder);

        try {
            Flux<Anime> topAnime = jikan.query().anime().top().execute();
            topAnime.toStream().forEach(anime -> {
                System.out.println(anime.getTitle());
                System.out.println(anime.getType());
            });
        } catch (JikanQueryException e) {
            e.printStackTrace();
        }
    }   

    public void loadVoiceActors(String writerPath) {
        String[] header = {"rank", "anime","character", "voiceactor"};
        CSVFormat format = CSVFormat.DEFAULT.builder().setHeader(header).build();

        System.out.println("Loading data into programm");
        Flux<Anime> topAnime = getRawData();
        topAnime.sort((anime1, anime2) -> Integer.compare(anime1.getMalId(), anime2.getMalId()));
        List<Integer> malIds = topAnime.toStream().map(Anime::getMalId).toList();
        List<String> titles = topAnime.toStream().map(Anime::getTitle).toList();
        List<Integer> ranks = topAnime.toStream().map(Anime::getRank).toList();
        List<List<String>> characters = new ArrayList<>();
        System.out.println("Done");

        Map<String, List<String>> rawData = new HashMap<>();

        try {

            int counter = 0;
            System.out.println("Loading voice Actors");
            for (int i = 0; i < malIds.size(); i++) {
                Flux<CharacterBasic> current = jikan.query().anime().characters(malIds.get(i)).execute();

                List<String> actors = current.toStream().filter(character -> character.getRole().equals(CharacterRole.MAIN))
                                  .flatMap(character -> character.getVoiceActors().stream())
                                  .filter(actor -> actor.getLanguage().equals("Japanese"))
                                  .map(actor -> actor.getPerson().getName())
                                  .toList();
                
                characters.add(current.toStream().filter(character -> character.getRole().equals(CharacterRole.MAIN))
                                                 .map(character -> character.getCharacter().getName()).toList());
                rawData.put(titles.get(i), actors);

                
                if(counter % 100 == 0) {
                    System.out.println("Finished: " + counter / 1000 +"%");
                }

                counter++;
            }

            System.out.println("Format data into csv");
            counter = 0;
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(writerPath));
            final CSVPrinter printer = new CSVPrinter(writer, format)) {

                for (String title : titles) {

                    int currentCharacter = 0;
                    List<String> animeCharacters = characters.get(counter); 
                    List<String> actors = rawData.get(title);

                    for (String actor : actors) {
                            currentCharacter += currentCharacter < animeCharacters.size() -1 ? 1 : 0;
                            printer.printRecord(ranks.get(counter), title, animeCharacters.get(currentCharacter), actor);
                    }

                    if(counter % 100 == 0) {
                        System.out.println("Finished: " + counter / 1000 +"%");
                    }

                    counter++;
                }

                printer.flush();

            } catch (IOException e) {
                e.printStackTrace();
            }

        } catch(JikanQueryException e) {
            e.printStackTrace();
        }   

    }
}