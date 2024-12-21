package sna.data;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        String[] animeHeader = {"id", "label", "name", "score", "rank"};

        // Speichern der Knoten
        loader.saveNodes("./data/Anime_Nodes.csv", animes, "Anime", animeHeader);

        // Speichern der Kanten
        loader.saveAnimeEdges("./data/Anime_Producers_Edges.csv", "producers");
        loader.saveAnimeEdges("./data/Anime_Studios_Edges.csv", "studios");
        // Abrufen der Synchronsprecher geht sehr lange, da für jeden Anime die Synchronsprecher abgerufen werden müssen
        // Zum testen kann die Zeile ".take(1000)" in der Methode "getRawData()"  auf ".take(10)" geändert werden
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
                .take(1000)// ändern auf 10 für schnelleres Testen
                .collectList()
                .block();
        return animeList;
    }


    private void saveNodes(String writerPath, List<?> datapoints, String label, String[] header) {
        CSVFormat format = CSVFormat.DEFAULT.builder().setHeader(header).build();

        System.out.println("Saving" + label + "nodes...");

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(writerPath));
            final CSVPrinter printer = new CSVPrinter(writer, format)) {
            int size = datapoints.size();
            String progress = "|";
            int counter = 0;

            for (Object data : datapoints) {
                // Display the progress
                int percentage = (int) ((counter / (double) size) * 100) + 1;
                if (counter % (size / 100) == 0 || counter == size - 1) {
                    progress += "=";
                    System.out.print("\r" + progress + "| " + percentage + "%");
    
                }

                Object[] record;
                
                if (data instanceof Anime) {
                    Anime anime = (Anime) data;
                    record = processAnime(anime);
                }

                printer.printRecord((Object[]) record);
                counter++;
            }

            // clean up messages
            System.out.println(" Done!");
            System.out.println("Formatting complete!");
            printer.flush();  
            System.out.println("CSV file was created successfully.");

        } catch (IOException e) {
            System.err.println("Error writing anime nodes to CSV: " + e.getMessage());
        }
    }

    private Object[] processAnime(Anime anime) {
        String malId = String.valueOf(anime.getMalId());
        String score = String.valueOf(anime.getScore());
        String title = anime.getTitle();
        String rank = String.valueOf(anime.getRank());
        String[] record = {malId, title, score, rank};
        return record;
    }

    private void saveAnimeEdges(String writerPath, String relationType) {
        String[] header = {"source", "target", "type"};
        CSVFormat format = CSVFormat.DEFAULT.builder().setHeader(header).build();

        System.out.println("Saving anime edges...");
        List<String[]> edges = getRawData().stream()
                .flatMap(anime -> {
                    int animeId = anime.getMalId();
                    List<String> relatedItems = relationType.equals("studios")
                            ? anime.getStudios().stream().map(Studio::getName).toList()
                            : anime.getProducers().stream().map(Producer::getName).toList();
                    return relatedItems.stream().map(item -> new String[]{String.valueOf(animeId), item, "undirected"});
                })
                .collect(Collectors.toList());

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(writerPath));
             CSVPrinter printer = new CSVPrinter(writer, format)) {

            for (String[] edge : edges) {
                printer.printRecord((Object[]) edge);
            }

            printer.flush();
            System.out.println("Anime edges saved successfully: " + writerPath);

        } catch (IOException e) {
            System.err.println("Error writing anime edges to CSV: " + e.getMessage());
        }
    }

    public void loadVoiceActorEdges(String writerPath) {
        String[] header = {"source", "target", "type"};
        CSVFormat format = CSVFormat.DEFAULT.builder().setHeader(header).build();

        System.out.println("Lade Daten in das Programm...");
        List<Anime> topAnime = getRawData();
        topAnime.sort(Comparator.comparingInt(MalEntity::getMalId));
        List<Integer> malIds = topAnime.stream().map(Anime::getMalId).toList();
        List<String> titles = topAnime.stream().map(Anime::getTitle).toList();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(writerPath));
             CSVPrinter printer = new CSVPrinter(writer, format)) {

            System.out.println("Lade Voice Actor-Verbindungen...");
            for (int i = 0; i < malIds.size(); i++) {
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
                        printer.printRecord(animeTitle, voiceActor, "undirected");
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
