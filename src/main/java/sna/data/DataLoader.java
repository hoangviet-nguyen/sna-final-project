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

        // Speichern der Knoten
        loader.saveAnimeNodes("./data/Anime_Nodes.csv");

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

    private void saveAnimeNodes(String writerPath) {
        String[] header = {"id", "title", "score", "rank"};
        CSVFormat format = CSVFormat.DEFAULT.builder().setHeader(header).build();

        System.out.println("Saving anime nodes...");
        List<Anime> animes = getRawData();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(writerPath));
             CSVPrinter printer = new CSVPrinter(writer, format)) {

            for (Anime anime : animes) {
                printer.printRecord(anime.getMalId(), anime.getTitle(), anime.getScore(), anime.getRank());
            }

            printer.flush();
            System.out.println("Anime nodes saved successfully: " + writerPath);

        } catch (IOException e) {
            System.err.println("Error writing anime nodes to CSV: " + e.getMessage());
        }
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



    //blic void loadVoiceActors(String writerPath) {
   //  String[] header = {"rank", "anime","character", "voiceactor"};
   //  CSVFormat format = CSVFormat.DEFAULT.builder().setHeader(header).build();

   //  System.out.println("Loading data into programm");
   //  List<Anime> topAnime = getRawData();
   //  topAnime.sort(Comparator.comparingInt(MalEntity::getMalId));
   //  List<Integer> malIds = topAnime.stream().map(Anime::getMalId).toList();
   //  List<String> titles = topAnime.stream().map(Anime::getTitle).toList();
   //  List<Integer> ranks = topAnime.stream().map(Anime::getRank).toList();
   //  List<List<String>> characters = new ArrayList<>();
   //  System.out.println("Done");

   //  Map<String, List<String>> rawData = new HashMap<>();

   //  try {

   //      int counter = 0;
   //      System.out.println("Loading voice Actors");
   //      for (int i = 0; i < malIds.size(); i++) {
   //          System.out.println("Fetching voice actors for: " + titles.get(i));
   //          Flux<CharacterBasic> current = jikan.query().anime().characters(malIds.get(i)).execute();

   //          List<String> actors = current.toStream().filter(character -> character.getRole().equals(CharacterRole.MAIN))
   //                            .flatMap(character -> character.getVoiceActors().stream())
   //                            .filter(actor -> actor.getLanguage().equals("Japanese"))
   //                            .map(actor -> actor.getPerson().getName())
   //                            .toList();
   //
   //          characters.add(current.toStream().filter(character -> character.getRole().equals(CharacterRole.MAIN))
   //                                           .map(character -> character.getCharacter().getName()).toList());
   //          rawData.put(titles.get(i), actors);

   //
   //          if(counter % 100 == 0) {
   //              System.out.println("Finished: " + counter / 1000 +"%");
   //          }

   //          counter++;
   //      }

   //      System.out.println("Format data into csv");
   //      counter = 0;
   //      try (BufferedWriter writer = new BufferedWriter(new FileWriter(writerPath));
   //      final CSVPrinter printer = new CSVPrinter(writer, format)) {

   //          for (String title : titles) {

   //              int currentCharacter = 0;
   //              List<String> animeCharacters = characters.get(counter);
   //              List<String> actors = rawData.get(title);

   //              for (String actor : actors) {
   //                      currentCharacter += currentCharacter < animeCharacters.size() -1 ? 1 : 0;
   //                      printer.printRecord(ranks.get(counter), title, animeCharacters.get(currentCharacter), actor);
   //              }

   //              if(counter % 100 == 0) {
   //                  System.out.println("Finished: " + counter / 1000 +"%");
   //              }

   //              counter++;
   //          }

   //          printer.flush();

   //      } catch (IOException e) {
   //          e.printStackTrace();
   //      }

   //  } catch(JikanQueryException e) {
   //      e.printStackTrace();
   //  }

    //}
}