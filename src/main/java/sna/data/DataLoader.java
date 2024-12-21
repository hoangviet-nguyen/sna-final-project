package sna.data;

import java.io.IOException;
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

    private static Jikan jikan;

    public DataLoader() { jikan = new Jikan();}

    public static void main (String[] args) {  
        DataLoader loader = new DataLoader();
        getRawData();
        //loader.loadStudios("./data/Anime_Studios.csv");
        //loader.loadProducers();
        //loader.loadVoiceActors("./data/VoiceActors.csv");
	}

    private static Flux<Anime> getRawData() {
        System.out.println("Fetching top anime Data");
        Flux<Anime> topAnime = Flux.range(1, 40)
        .concatMap(page -> {
            try {
                return jikan.query().anime().top().limit(25).page(page).execute();
            } catch (JikanQueryException e) {
                throw new RuntimeException(e);
            }
        });
        System.out.println(" Done!");
        System.out.println("Loading complete!");
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
        int counter = 1;
        topAnime.toStream().forEach(anime -> {
            System.out.println("\r" + "Processing anime: " + counter);
            String title = anime.getTitle();
            String rank = String.valueOf(anime.getRank());
            String type = anime.getType().getSearch();
            String[] rank_title = {rank, title, type};
            List<String> studios = anime.getStudios().stream().map(Studio::getName).toList();
            data.put(rank_title, studios);
            counter++;
        });
        System.out.println("Done");     

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(writerPath));
            final CSVPrinter printer = new CSVPrinter(writer, format)) {
            
            System.out.println("Writing data to csv");
            int counter = 0;
            int fetchedAnimes = data.size();
            final StringBuilder progress = new StringBuilder("|");
            data.forEach((rank_title, studios) -> {
                studios.stream().forEach(studio -> {
                    try {
                        printer.printRecord(rank_title[0], rank_title[1],rank_title[2], studio);
                    } catch (IOException ioe) {
                        ioe.printStackTrace();
                    }
                });

                
                int percentage = (int) ((counter / (double) fetchedAnimes) * 100) + 1;
                if (counter % (fetchedAnimes / 100) == 0 || counter == fetchedAnimes - 1) {
                    progress.append("=");
                    System.out.print("\r" + progress.toString() + "| " + percentage + "%");
                }
            });
            System.out.println(" Done!");
            printer.flush();  
            System.out.println("CSV file was created successfully.");

        } catch (IOException ioe) {
            System.out.println(ioe.getMessage());
        }

    } 

    public void loadVoiceActors(String writerPath) {
        String[] header = {"rank", "anime", "voiceactor"};
        CSVFormat format = CSVFormat.DEFAULT.builder().setHeader(header).build();

        System.out.println("Loading data into programm");
        Flux<Anime> topAnime = getRawData();
        topAnime.sort((anime1, anime2) -> Integer.compare(anime1.getMalId(), anime2.getMalId()));
        List<Integer> malIds = topAnime.toStream().map(Anime::getMalId).toList();
        List<String> titles = topAnime.toStream().map(Anime::getTitle).toList();
        List<Integer> ranks = topAnime.toStream().map(Anime::getRank).toList();
        System.out.println("Done");

        Map<String, List<String>> rawData = new HashMap<>();

        try {

            int counter = 0;
            System.out.println("Loading voice Actors");
            String progress = "|";
            int malIdSize = malIds.size();
            for (int i = 0; i < malIdSize; i++) {

                Flux<CharacterBasic> current = jikan.query().anime().characters(malIds.get(i)).execute();

                List<String> actors = current.toStream().filter(character -> character.getRole().equals(CharacterRole.MAIN))
                                  .flatMap(character -> character.getVoiceActors().stream())
                                  .filter(actor -> actor.getLanguage().equals("Japanese"))
                                  .map(actor -> actor.getPerson().getName())
                                  .toList();
                
                rawData.put(titles.get(i), actors);

                
                int percentage = (int) ((i / (double) malIdSize) * 100) + 1;
                if (i % (malIdSize / 100) == 0 || i == malIdSize - 1) {
                    progress += "=";
                    System.out.print("\r" + progress + "| " + percentage + "%");
    
                }
    
                counter++;
            }
            System.out.println(" Done!");
            System.out.println("Loading complete!");

            System.out.println("Format data into csv");
            counter = 0;
            int fetchedAnimes = titles.size();
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(writerPath));
            final CSVPrinter printer = new CSVPrinter(writer, format)) {

                for (String title : titles) {

                    List<String> actors = rawData.get(title);
                    for (String actor : actors) {
                            printer.printRecord(ranks.get(counter), title, actor);
                    }

                    int percentage = (int) ((counter / (double) fetchedAnimes) * 100) + 1;
                    if (counter % (fetchedAnimes / 100) == 0 || counter == fetchedAnimes - 1) {
                        progress += "=";
                        System.out.print("\r" + progress + "| " + percentage + "%");
        
                    }

                    counter++;
                }
                System.out.println(" Done!");
                System.out.println("Formatting complete!");
                printer.flush();

            } catch (IOException e) {
                e.printStackTrace();
            }

        } catch(JikanQueryException e) {
            e.printStackTrace();
        }   

    }
}