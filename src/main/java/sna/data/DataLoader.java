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

public static void main(String[] args) {
    long startTime = System.currentTimeMillis();
    DataLoader loader = new DataLoader();

    List<Anime> animes = loader.getRawData();

    // Save anime nodes
    loader.saveNodes("./data/Anime_Nodes.csv", "anime", animes);

    // Save producer nodes
    loader.saveNodes("./data/Producer_Nodes.csv", "producers", animes);

    // Save studio nodes
    loader.saveNodes("./data/Studio_Nodes.csv", "studios", animes);

    // Save anime-producer relations
    loader.saveAnimeRelations("./data/Anime_Producer_Relations.csv", "producers");

    // Save anime-studio relations
    loader.saveAnimeRelations("./data/Anime_Studio_Relations.csv", "studios");

    // Load voice actor edges
    loader.loadVoiceActorEdges("./data/VoiceActors_Edges.csv");

    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;
    System.out.println("Total time taken: " + duration / 1000 + " seconds");
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
                .take(10)// ändern auf 10 für schnelleres Testen
                .collectList()
                .block();
        return animeList;
    }

    private void saveNodes(String writerPath, String nodeType, List<Anime> animes) {
        String[] header = nodeType.equals("anime") ? new String[]{"id", "label", "name", "score", "rank"} : new String[]{"id", "label", "name"};
        CSVFormat format = CSVFormat.DEFAULT.builder().setHeader(header).build();

        System.out.println("Saving " + nodeType + " nodes...");
        Set<String> uniqueNames = new HashSet<>(); // To avoid duplicates

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(writerPath));
             CSVPrinter printer = new CSVPrinter(writer, format)) {

            for (Anime anime : animes) {
                if (nodeType.equals("anime")) {
                    printer.printRecord(anime.getMalId(), "Anime", anime.getTitle(), anime.getScore(), anime.getRank());
                } else {
                    List<String> items = nodeType.equals("studios")
                            ? anime.getStudios().stream().map(Studio::getName).toList()
                            : anime.getProducers().stream().map(Producer::getName).toList();
                    for (String item : items) {
                        if (uniqueNames.add(item)) { // Prevent duplicates
                            printer.printRecord(item.hashCode(), nodeType.substring(0, 1).toUpperCase() + nodeType.substring(1), item);
                        }
                    }
                }
            }

            printer.flush();
            System.out.println(nodeType + " nodes saved successfully: " + writerPath);

        } catch (IOException e) {
            System.err.println("Error writing " + nodeType + " nodes to CSV: " + e.getMessage());
        }
    }

    private void saveAnimeRelations(String writerPath, String relationType) {
        String[] header = {"source", "target", "type"};
        CSVFormat format = CSVFormat.DEFAULT.builder().setHeader(header).build();

        System.out.println("Saving anime " + relationType + " relations...");
        List<String[]> edges = getRawData().stream()
                .flatMap(anime -> {
                    int animeId = anime.getMalId();
                    List<String> relatedItems = relationType.equals("studios")
                            ? anime.getStudios().stream().map(Studio::getName).toList()
                            : anime.getProducers().stream().map(Producer::getName).toList();
                    return relatedItems.stream().map(item -> new String[]{String.valueOf(animeId), String.valueOf(item.hashCode()), relationType});
                })
                .collect(Collectors.toList());

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(writerPath));
             CSVPrinter printer = new CSVPrinter(writer, format)) {

            for (String[] edge : edges) {
                printer.printRecord((Object[]) edge);
            }

            printer.flush();
            System.out.println(relationType + " relations saved successfully: " + writerPath);

        } catch (IOException e) {
            System.err.println("Error writing anime " + relationType + " relations to CSV: " + e.getMessage());
        }
    }



    public void loadVoiceActorEdges(String writerPath) {
        String[] header = {"source", "target", "type"};
        CSVFormat format = CSVFormat.DEFAULT.builder().setHeader(header).build();

        System.out.println("Lade Daten in das Programm...");
        List<Anime> topAnime = getRawData();
        topAnime.sort(Comparator.comparingInt(MalEntity::getMalId));
        List<Integer> malIds = topAnime.stream().map(Anime::getMalId).toList();
        List<Integer> animeIds = topAnime.stream().map(Anime::getMalId).toList();
        List<String> titles = topAnime.stream().map(Anime::getTitle).toList();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(writerPath));
             CSVPrinter printer = new CSVPrinter(writer, format)) {

            System.out.println("Lade Voice Actor-Verbindungen...");
            for (int i = 0; i < malIds.size(); i++) {
                String animeId = String.valueOf(animeIds.get(i));
                String animeTitle = titles.get(i);
                System.out.println("Abrufen von Synchronsprechern für: " + animeTitle);
                try {
                    Flux<CharacterBasic> current = jikan.query().anime().characters(malIds.get(i)).execute();

                    List<String> voiceActors = current.toStream()
                            .filter(character -> character.getRole().equals(CharacterRole.MAIN))
                            .flatMap(character -> character.getVoiceActors().stream())
                            .filter(actor -> actor.getLanguage().equals("Japanese"))
                            .map(actor -> actor.getPerson().getName())
                            .toList();

                    for (String voiceActor : voiceActors) {
                        // Schreibe jede Verbindung in die CSV-Datei
                        printer.printRecord(String.valueOf(animeId), voiceActor, "undirected");
                    }

                } catch (JikanQueryException e) {
                    System.err.println("Fehler beim Abrufen von Voice Actors für " + animeTitle + ": " + e.getMessage());
                }

                if (i % 10 == 0) {
                    System.out.println("Fortschritt: " + i + "/" + malIds.size());
                }
            }

            printer.flush();
            System.out.println("Voice Actor-Edges CSV wurde erfolgreich erstellt: " + writerPath);

        } catch (IOException e) {
            System.err.println("Fehler beim Schreiben der CSV-Datei: " + e.getMessage());
        }
    }

}