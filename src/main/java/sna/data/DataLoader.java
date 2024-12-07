package sna.data;

import java.io.IOException;
import java.util.*;

import net.sandrohc.jikan.model.MalEntity;
import net.sandrohc.jikan.model.anime.AnimeType;
import net.sandrohc.jikan.model.common.Producer;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import net.sandrohc.jikan.Jikan;
import net.sandrohc.jikan.exception.JikanQueryException;
import net.sandrohc.jikan.model.anime.Anime;
import net.sandrohc.jikan.model.character.CharacterBasic;
import net.sandrohc.jikan.model.character.CharacterRole;
import net.sandrohc.jikan.model.common.Studio;
import reactor.core.publisher.Flux;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.stream.Collectors;

public class DataLoader {

    private Jikan jikan;
    private List<Anime> animeList;

    public DataLoader() { jikan = new Jikan();}

    public static void main (String[] args) {
        long startTime = System.currentTimeMillis();
        DataLoader loader = new DataLoader();
        loader.loadData("./data/Anime_Producers.csv", "producers");
        loader.loadData("./data/Anime_Studios.csv", "studios");
        loader.loadVoiceActors("./data/VoiceActors.csv");

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        System.out.println("Total time taken: " + duration/1000 + " seconds");
	}

    private List<Anime> getRawData() {
        if (animeList != null && !animeList.isEmpty()) {
            return animeList;
        }
        System.out.println("Fetching anime data...");
        animeList = Flux.range(1, Integer.MAX_VALUE)
                .concatMap(page -> {
                    System.out.println("Fetching page " + page);
                    try {
                        return jikan.query().anime().top().limit(25).page(page).execute();
                    } catch (JikanQueryException e) {
                        throw new RuntimeException("Error fetching data from Jikan API", e);
                    }
                })
                .filter(anime -> anime.getType() == AnimeType.TV || anime.getType() == AnimeType.MOVIE)
                .take(1000)
                .collectList()
                .block();
        return animeList;
    }


    private void loadData(String writerPath, String dataType) {
        String[] header = {"rank", "anime", dataType};
        CSVFormat format = CSVFormat.DEFAULT.builder().setHeader(header).build();

        System.out.println("Loading and formatting data...");
        List<String[]> records = getRawData().stream()
                .flatMap(anime -> {
                    String rank = String.valueOf(anime.getRank());
                    String title = anime.getTitle();
                    List<String> dataItems = dataType.equals("studios")
                            ? anime.getStudios().stream().map(Studio::getName).toList()
                            : anime.getProducers().stream().map(Producer::getName).toList();
                    return dataItems.stream().map(item -> new String[]{rank, title, item});
                })
                .collect(Collectors.toList());
        System.out.println("Data formatted successfully.");

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(writerPath));
             CSVPrinter printer = new CSVPrinter(writer, format)) {

            System.out.println("Writing data to CSV...");
            for (String[] record : records) {
                printer.printRecord((Object[]) record);
            }
            printer.flush();
            System.out.println("CSV file created successfully: " + writerPath);

        } catch (IOException e) {
            System.err.println("Error writing to CSV: " + e.getMessage());
        }
    }



    public void loadVoiceActors(String writerPath) {
        String[] header = {"rank", "anime","character", "voiceactor"};
        CSVFormat format = CSVFormat.DEFAULT.builder().setHeader(header).build();

        System.out.println("Loading data into programm");
        List<Anime> topAnime = getRawData();
        topAnime.sort(Comparator.comparingInt(MalEntity::getMalId));
        List<Integer> malIds = topAnime.stream().map(Anime::getMalId).toList();
        List<String> titles = topAnime.stream().map(Anime::getTitle).toList();
        List<Integer> ranks = topAnime.stream().map(Anime::getRank).toList();
        List<List<String>> characters = new ArrayList<>();
        System.out.println("Done");

        Map<String, List<String>> rawData = new HashMap<>();

        try {

            int counter = 0;
            System.out.println("Loading voice Actors");
            for (int i = 0; i < malIds.size(); i++) {
                System.out.println("Fetching voice actors for: " + titles.get(i));
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