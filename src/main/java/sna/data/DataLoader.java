package sna.data;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import net.sandrohc.jikan.model.MalEntity;
import net.sandrohc.jikan.model.anime.AnimeType;
import net.sandrohc.jikan.model.common.Producer;
import net.sandrohc.jikan.model.person.PersonSimple;
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
import java.io.File;

public class DataLoader {

    private Jikan jikan;
    private List<Anime> animeList;
    private List<Studio> studioList = new ArrayList<>();
    Map<Anime, List<PersonSimple>> voiceActorsMap = new HashMap<>();

    public DataLoader() { jikan = new Jikan();}

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        CleanDataDir();

        DataLoader loader = new DataLoader();

        loader.fetchRawData();
        loader.mapAnimeActor(loader.animeList);
        // Speichern der Knoten
        String[] animeHeader = {"id", "label", "name", "score", "rank"};
        String[] studioHeader = {"id", "label", "name"};
        loader.saveNodes("./data/Anime_Nodes.csv", loader.animeList, "Anime", animeHeader);
        loader.saveNodes("./data/Studio_Nodes.csv", loader.studioList, "Studio", studioHeader);

        // Speichern der Kanten
        loader.saveAnimeEdges("./data/Anime_Producers_Edges.csv", "producers");
        loader.saveAnimeEdges("./data/Anime_Studios_Edges.csv", "studios");
        // Abrufen der Synchronsprecher geht sehr lange, da für jeden Anime die Synchronsprecher abgerufen werden müssen
        // Zum testen kann die Zeile ".take(1000)" in der Methode "getRawData()"  auf ".take(10)" geändert werden

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        System.out.println("Total time taken: " + duration / 1000 + " seconds");
    }



    private  void fetchRawData() {
        if (animeList != null && !animeList.isEmpty()) {
            return;
        }
        System.out.println("Fetching anime data...");
        animeList = Flux.range(1, Integer.MAX_VALUE)
                .concatMap(page -> {
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
        animeList.forEach(anime -> {
            studioList.addAll(anime.getStudios());
        });
    }


    private void saveNodes(String writerPath, List<?> datapoints, String label, String[] header) {
        CSVFormat format = CSVFormat.DEFAULT.builder().setHeader(header).build();
        boolean fileExists = Files.exists(Paths.get(writerPath));
        System.out.println("Saving" + label + "nodes...");

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(writerPath, fileExists));
            final CSVPrinter printer = fileExists ? new CSVPrinter(writer, CSVFormat.DEFAULT) : new CSVPrinter(writer, format)) {

            int size = datapoints.size();
            String progress = "|";
            int counter = 0;

            for (Object data : datapoints) {
                // Display the progress
                int percentage = (int) ((counter / (double) size) * 100) + 1;
                if (counter %  (size < 100 ? size : (size / 100)) == 0 || counter == size - 1) {
                    progress += "=";
                    System.out.print("\r" + progress + "| " + percentage + "%");

                }

                Object[] record = null;
                
                if (data instanceof Anime anime) {
                    record = processAnime(anime);
                }
                else if (data instanceof PersonSimple voiceActor) {
                    record = new String[]{String.valueOf(voiceActor.getMalId()), "Voice-Actor", voiceActor.getName()};
                }else if (data instanceof Studio studio) {
                    record = new String[]{String.valueOf(studio.getMalId()), "Studio", studio.getName()};
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
        List<String[]> edges = animeList.stream()
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

    private void mapAnimeActor(List<Anime> anime){
        System.out.println("Lade Voice Actor-Verbindungen...");
        String[] voiceActorHeader = {"id", "label", "name"};
        for (Anime currentAnime : anime) {
            String animeTitle = currentAnime.getTitle();
            System.out.println("Abrufen von Synchronsprechern für: " + animeTitle);
            try {
                Flux<CharacterBasic> current = jikan.query().anime().characters(currentAnime.malId).execute();

                List<PersonSimple> voiceActors = current.toStream()
                        .filter(character -> character.getRole().equals(CharacterRole.MAIN))
                        .flatMap(character -> character.getVoiceActors().stream())
                        .filter(actor -> actor.getLanguage().equals("Japanese"))
                        .map(actor -> actor.getPerson())
                        .toList();
                saveNodes("./data/VoiceActor_Nodes.csv"  , voiceActors, "Voice-Actor", voiceActorHeader);
                voiceActorsMap.put(currentAnime, voiceActors);

            } catch (JikanQueryException e) {
                System.err.println("Fehler beim Abrufen von Voice Actors für " + animeTitle + ": " + e.getMessage());
            }
        }


    }


    // Löscht alle Dateien im data-Ordner, da die neuen rows im VoiceActor-File appended
    private static void CleanDataDir() {
        try {
            System.out.println("Bereite Datenordner vor...");
            Files.walk(Paths.get("./data"))
                    .filter(Files::isRegularFile)
                    .map(java.nio.file.Path::toFile)
                    .forEach(File::delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
