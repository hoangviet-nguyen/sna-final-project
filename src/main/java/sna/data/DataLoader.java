package sna.data;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import net.sandrohc.jikan.Jikan;
import net.sandrohc.jikan.exception.JikanQueryException;
import net.sandrohc.jikan.model.anime.Anime;
import net.sandrohc.jikan.model.common.Studio;
import reactor.core.publisher.Flux;

import java.io.BufferedWriter;
import java.io.FileWriter;

public class DataLoader {

    private Jikan jikan;

    public DataLoader() { jikan = new Jikan();}

    public static void main (String[] args) {  
        DataLoader loader = new DataLoader();
        loader.loadStudios("./data/Anime_Studio.csv");
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
        String[] header = {"rank", "anime", "studios"};
        Map<String[], List<String>> data = new HashMap<>();
        CSVFormat format = CSVFormat.DEFAULT.builder().setHeader(header).build();

        System.out.println("Loading data into programm");
        Flux<Anime> topAnime = getRawData();
        System.out.println("Done");

        System.out.println("Format data for csv");
        topAnime.toStream().forEach(anime -> {
            String title = anime.getTitle();
            String rank = String.valueOf(anime.getRank());
            String[] rank_title = {rank, title};
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
                        printer.printRecord(rank_title[0], rank_title[1], studio);
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

    }

    public void loadVoiceActors() {
        
    }
}