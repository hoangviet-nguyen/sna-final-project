package sna.data;

import it.uniroma1.dis.wsngroup.gexf4j.core.EdgeType;
import it.uniroma1.dis.wsngroup.gexf4j.core.Gexf;
import it.uniroma1.dis.wsngroup.gexf4j.core.Graph;
import it.uniroma1.dis.wsngroup.gexf4j.core.Mode;
import it.uniroma1.dis.wsngroup.gexf4j.core.Node;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.GexfImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.StaxGraphWriter;
import net.sandrohc.jikan.Jikan;
import net.sandrohc.jikan.exception.JikanQueryException;
import net.sandrohc.jikan.model.anime.Anime;
import net.sandrohc.jikan.model.common.Producer;
import reactor.core.publisher.Flux;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

public class Main {

    public static void main(String[] args) throws JikanQueryException {
        System.out.println("Fetching anime and producers...");

        Jikan jikan = new Jikan();
        Gexf gexf = new GexfImpl();
        Calendar date = Calendar.getInstance();

        gexf.getMetadata()
                .setLastModified(date.getTime())
                .setCreator("Anime Network")
                .setDescription("Producers to Anime Network");
        gexf.setVisualization(true);

        Graph graph = gexf.getGraph();
        graph.setDefaultEdgeType(EdgeType.UNDIRECTED).setMode(Mode.STATIC);

        Set<String> nodeIds = new HashSet<>();
        //Sample code for creating a two node network with Producer -> Anime relationship
        Flux<Anime> topAnime = Flux.range(1, 4)
                .concatMap(page -> {
                    System.out.println("Fetching page " + page);
                    try {
                        return jikan.query().anime().top().limit(25).page(page).execute();
                    } catch (JikanQueryException e) {
                        throw new RuntimeException(e);
                    }
                });

        topAnime.collectList().block().forEach(anime -> {
            String animeId = "anime_" + anime.getMalId();
            if (!nodeIds.contains(animeId)) {
                Node animeNode = graph.createNode(animeId);
                animeNode.setLabel(anime.getTitle());
                nodeIds.add(animeId);
            }

            for (Producer producer : anime.getProducers()) {
                String producerId = "producer_" + producer.getMalId();
                Node producerNode;
                if (!nodeIds.contains(producerId)) {
                    producerNode = graph.createNode(producerId);
                    producerNode.setLabel(producer.getName());
                    nodeIds.add(producerId);
                } else {
                    producerNode = graph.getNode(producerId);
                }

                Node animeNode = graph.getNode(animeId);
                if (animeNode != null) {
                    producerNode.connectTo(animeNode);
                }
            }

        });

        try {
            StaxGraphWriter graphWriter = new StaxGraphWriter();
            File outputFile = new File("anime_producer_network.gexf");
            Writer out = new FileWriter(outputFile, false);
            graphWriter.writeToStream(gexf, out, "UTF-8");
            System.out.println("Graph saved to: " + outputFile.getAbsolutePath());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
