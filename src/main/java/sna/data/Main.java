package sna.data;

import it.uniroma1.dis.wsngroup.gexf4j.core.*;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.Attribute;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeClass;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeList;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeType;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.GexfImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.StaxGraphWriter;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.data.AttributeListImpl;
import net.sandrohc.jikan.Jikan;
import net.sandrohc.jikan.exception.JikanQueryException;
import net.sandrohc.jikan.model.anime.Anime;
import net.sandrohc.jikan.model.common.Studio;
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

        AttributeList nodeAttributes = new AttributeListImpl(AttributeClass.NODE);
        graph.getAttributeLists().add(nodeAttributes);
        Attribute typeAttr = nodeAttributes.createAttribute("type", AttributeType.STRING, "Type");

        Set<String> nodeIds = new HashSet<>();
        Set<String> edgeIds = new HashSet<>();


        
        // Fetch top 40 pages of anime, resulting in top 1000 anime
        Flux<Anime> topAnime = Flux.range(1, 40)
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
            Node animeNode;

            if (!nodeIds.contains(animeId)) {
                animeNode = graph.createNode(animeId);
                animeNode.setLabel(anime.getTitle());
                animeNode.getAttributeValues().addValue(typeAttr, "Anime");
                nodeIds.add(animeId);
            } else {
                animeNode = graph.getNode(animeId);
            }

            for (Studio studio : anime.getStudios()) {
                String studioId = "studio_" + studio.getMalId();
                Node studioNode;

                if (!nodeIds.contains(studioId)) {
                    studioNode = graph.createNode(studioId);
                    studioNode.setLabel(studio.getName());
                    studioNode.getAttributeValues().addValue(typeAttr, "Studio");
                    nodeIds.add(studioId);
                } else {
                    studioNode = graph.getNode(studioId);
                }

                String edgeId = studioId + "->" + animeId;
                if (!edgeIds.contains(edgeId)) {
                    studioNode.connectTo(animeNode);
                    edgeIds.add(edgeId);
                }
            }
        });

        try {
            StaxGraphWriter graphWriter = new StaxGraphWriter();
            File outputFile = new File("anime_studio_network.gexf");
            Writer out = new FileWriter(outputFile, false);
            graphWriter.writeToStream(gexf, out, "UTF-8");
            System.out.println("Graph saved to: " + outputFile.getAbsolutePath());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

