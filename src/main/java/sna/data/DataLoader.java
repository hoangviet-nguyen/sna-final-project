package sna.data;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import net.sandrohc.jikan.model.anime.AnimeType;
import net.sandrohc.jikan.model.character.CharacterVoiceActor;
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
import java.io.File;


public class DataLoader {

    private final Jikan jikan;
    private final List<Anime> animeList;
    private final Set<Studio> studioSet;
    private final Set<PersonSimple> personSimpleSet;
    private final Map<Anime, List<PersonSimple>> voiceActorsMap;

    public DataLoader() {
        jikan = new Jikan();
        animeList = new ArrayList<>();
        studioSet = new HashSet<>();
        personSimpleSet = new HashSet<>();
        voiceActorsMap = new HashMap<>();
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        log("Starting data loading process...");

        // Cleans the data directory
        cleanDataDir();

        DataLoader loader = new DataLoader();

        // Fetches the raw data from the Jikan API
        loader.fetchRawData();

        // Maps the voice actors for each anime
        loader.mapAnimeActor(loader.animeList);

        // Defines the headers for the CSV files
        String[] animeHeader = {"id", "label", "name", "score", "rank"};
        String[] studioHeader = {"id", "label", "name"};
        String[] voiceActorHeader = {"id", "label", "name"};

        // Saves the nodes
        loader.saveNodes("./data/VoiceActor_Nodes.csv", loader.personSimpleSet.stream().toList(), "Voice-Actor", voiceActorHeader);
        loader.saveNodes("./data/Anime_Nodes.csv", loader.animeList, "Anime", animeHeader);
        loader.saveNodes("./data/Studio_Nodes.csv", loader.studioSet.stream().toList(), "Studio", studioHeader);

        // Saves the relations
        loader.saveRelation("./data/VoiceActor_Relations.csv", "voice-actors");
        loader.saveRelation("./data/Studio_Relations.csv", "studios");

        long endTime = System.currentTimeMillis();
        log("Total time taken: " + (endTime - startTime) / 1000 + " seconds");
    }

    private void fetchRawData() {
        if (!animeList.isEmpty()) {
            return;
        }
        log("Fetching anime data...");
        Set<Integer> uniqueAnimeIds = new HashSet<>();

        Flux.range(1, Integer.MAX_VALUE)
                .concatMap(page -> {
                    try {
                        return jikan.query().anime().top().limit(25).page(page).execute();
                    } catch (JikanQueryException e) {
                        throw new RuntimeException("Error fetching data from Jikan API", e);
                    }
                })
                .filter(anime -> anime.getType() == AnimeType.TV || anime.getType() == AnimeType.MOVIE)
                .takeWhile(anime -> {
                    if (uniqueAnimeIds.add(anime.getMalId())) {
                        animeList.add(anime);
                    }
                    return uniqueAnimeIds.size() < 1000; // This defines the number of anime to fetch. For testing purposes, we recommend setting this to 10.
                })
                .blockLast();

        animeList.forEach(anime -> studioSet.addAll(anime.getStudios()));
        log("Anime data fetching complete.");
    }

    private void saveNodes(String writerPath, List<?> datapoints, String label, String[] header) {
        CSVFormat format = CSVFormat.DEFAULT.builder().setHeader(header).build();
        boolean fileExists = Files.exists(Paths.get(writerPath));
        log("Saving " + label + " nodes...");

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(writerPath, fileExists));
             CSVPrinter printer = fileExists ? new CSVPrinter(writer, CSVFormat.DEFAULT) : new CSVPrinter(writer, format)) {

            ProgressBar progressBar = new ProgressBar(datapoints.size(), "Saving " + label + " nodes");

            for (Object data : datapoints) {
                Object[] record = null;

                if (data instanceof Anime anime) {
                    record = processAnime(anime, label);
                } else if (data instanceof PersonSimple voiceActor) {
                    record = new String[]{String.valueOf(voiceActor.getMalId()), label, voiceActor.getName()};
                } else if (data instanceof Studio studio) {
                    record = new String[]{String.valueOf(studio.getMalId()), label, studio.getName()};
                }

                printer.printRecord((Object[]) record);
                progressBar.update();
            }

            progressBar.complete();
            log(label + " nodes saved successfully.");
        } catch (IOException e) {
            logError("Error writing " + label + " nodes to CSV: " + e.getMessage());
        }
    }

    private Object[] processAnime(Anime anime, String label) {
        return new String[]{
                String.valueOf(anime.getMalId()),
                label,
                anime.getTitle(),
                String.valueOf(anime.getScore()),
                String.valueOf(anime.getRank())
        };
    }

    private void mapAnimeActor(List<Anime> anime) {
        log("Fetching Voice Actors...");
        ProgressBar progressBar = new ProgressBar(anime.size(), "Fetching Voice Actors");

        for (Anime currentAnime : anime) {
            try {
                Flux<CharacterBasic> current = jikan.query().anime().characters(currentAnime.malId).execute();

                List<PersonSimple> voiceActors = current.toStream()
                        .filter(character -> character.getRole().equals(CharacterRole.MAIN))
                        .flatMap(character -> character.getVoiceActors().stream())
                        .filter(actor -> actor.getLanguage().equals("Japanese"))
                        .map(CharacterVoiceActor::getPerson)
                        .toList();

                personSimpleSet.addAll(voiceActors);
                voiceActorsMap.put(currentAnime, voiceActors);

            } catch (JikanQueryException e) {
                logError("Error fetching voice actors for anime " + currentAnime.getTitle() + ": " + e.getMessage());
            }
            progressBar.update();
        }

        progressBar.complete();
        log("Voice Actor fetching complete.");
    }

    private void saveRelation(String writerPath, String relationType) {
        String[] header = {"source", "target", "type"};
        CSVFormat format = CSVFormat.DEFAULT.builder().setHeader(header).build();
        log("Saving " + relationType + " relations...");

        List<List<String>> edges = switch (relationType) {
            case "voice-actors" -> voiceActorsMap.entrySet().stream()
                    .flatMap(entry -> entry.getValue().stream()
                            .map(actor -> List.of(String.valueOf(entry.getKey().getMalId()), String.valueOf(actor.getMalId()), "undirected")))
                    .toList();
            case "studios" -> animeList.stream()
                    .flatMap(anime -> anime.getStudios().stream()
                            .map(studio -> List.of(String.valueOf(anime.getMalId()), String.valueOf(studio.getMalId()), "undirected")))
                    .toList();
            default -> throw new IllegalArgumentException("Unknown relation type: " + relationType);
        };

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(writerPath));
             CSVPrinter printer = new CSVPrinter(writer, format)) {
            for (List<String> edge : edges) {
                printer.printRecord(edge);
            }
            log(relationType + " relations saved successfully: " + writerPath);
        } catch (IOException e) {
            logError("Error writing " + relationType + " relations to CSV: " + e.getMessage());
        }
    }

    private static void cleanDataDir() {
        try {
            log("Cleaning data directory...");
            Files.walk(Paths.get("./data"))
                    .filter(Files::isRegularFile)
                    .map(java.nio.file.Path::toFile)
                    .forEach(File::delete);
            log("Data directory cleaned.");
        } catch (IOException e) {
            logError("Error cleaning data directory: " + e.getMessage());
        }
    }

    private static void log(String message) {
        System.out.println("[INFO] [" + new Date() + "] " + message);
    }

    private static void logError(String message) {
        System.err.println("[ERROR] [" + new Date() + "] " + message);
    }

    static class ProgressBar {
        private final int total;
        private int current = 0;
        private final String taskName;

        public ProgressBar(int total, String taskName) {
            this.total = total;
            this.taskName = taskName;
        }


        public void complete() {
            System.out.println();
            log(taskName + " complete.");
        }

        public void update() {
            current++;
            int barWidth = 50;
            int progress = (int) (barWidth * ((double) current / total));
            System.out.print("\r" + taskName + " [" + "#".repeat(progress) + " ".repeat(barWidth - progress) + "] " + current + "/" + total);

        }
    }
}
