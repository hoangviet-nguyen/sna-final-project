package sna.data;

import it.uniroma1.dis.wsngroup.gexf4j.core.EdgeType;
import it.uniroma1.dis.wsngroup.gexf4j.core.Gexf;
import it.uniroma1.dis.wsngroup.gexf4j.core.Graph;
import it.uniroma1.dis.wsngroup.gexf4j.core.Mode;
import it.uniroma1.dis.wsngroup.gexf4j.core.Node;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.Attribute;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeClass;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeList;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeType;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.GexfImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.StaxGraphWriter;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.data.AttributeListImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.viz.NodeShape;
import net.sandrohc.jikan.Jikan;
import net.sandrohc.jikan.exception.JikanQueryException;
import net.sandrohc.jikan.model.anime.Anime;
import reactor.core.publisher.Flux;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.time.LocalDate;
import java.util.Calendar;
import java.sql.Date;


public class example {

    public static void main (String[] args) {

        System.out.println("Fetching anime and producers...");

        Jikan jikan = new Jikan();

                //Sample code for creating a two node network with Producer -> Anime relationship
        Flux<Anime> topAnime = Flux.range(1, 2)
                .concatMap(page -> {
                    try {
                        return jikan.query().anime().top().limit(25).page(page).execute();
                    } catch (JikanQueryException e) {
                        throw new RuntimeException(e);
                    }
                });
        
        System.out.println("Displaying the first few entries: ");

        topAnime.toStream().forEach(System.out::println);


        Gexf gexf = new GexfImpl();
        gexf.getMetadata()
            .setCreator("Your Name")
            .setDescription("2-Mode Network Example")
            .setLastModified(Date.valueOf(LocalDate.now()));
        Graph graph = gexf.getGraph();
        graph.setDefaultEdgeType(EdgeType.UNDIRECTED)
             .setMode(Mode.STATIC);  // Static 2-mode network
        
             
	}
}