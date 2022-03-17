package com.example.demo;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.*;
import java.util.stream.Collectors;


public class CSVUtilTest {

    @Test
    void converterData(){
        List<Player> list = CsvUtilFile.getPlayers();
        assert list.size() == 18207;
    }


    //Filrta los jugadores mayores a 35 con stream
    @Test
    void stream_filtrarJugadoresMayoresA35(){
        List<Player> list = CsvUtilFile.getPlayers();
        Map<String, List<Player>> listFilter = list.parallelStream()
                .filter(player -> player.age >= 35)
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    return player;
                })
                .flatMap(playerA -> list.parallelStream()
                        .filter(playerB -> playerA.club.equals(playerB.club))
                )
                .distinct()
                .collect(Collectors.groupingBy(Player::getClub));

        assert listFilter.size() == 322;
    }
//Filrta los jugadores mayores a 35 reactor que sean del mismo equipo

    @Test
    void reactive_filtrarJugadoresMayoresA35(){
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .filter(player -> player.age >= 35)
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    return player;
                })
                .buffer(100)
                .flatMap(playerA -> listFlux
                         .filter(playerB -> playerA.stream()
                                 .anyMatch(a ->  a.club.equals(playerB.club)))
                )
                .distinct()
                .collectMultimap(Player::getClub);

        assert listFilter.block().size() == 322;
    }

//Filrta los jugadores mayores a 34 reactor
@Test
void reactive_filtrarJugadoresMayoresA34() {
    List<Player> list = CsvUtilFile.getPlayers();
    Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
    Mono<Map<String, Collection<Player>>> listFilter = listFlux
            .filter(player -> player.age > 34 && player.club.equals("FC Schalke 04"))
            .distinct()
            .collectMultimap(Player::getClub);

}


//Filtra a los jugadores por ranking segun su nacionalidad con reactor
    @Test
    void reactive_filtrasRankingNacionalidad() {
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        listFlux
                .sort((player1, player2) -> Math.max(player1.winners, player2.winners))
                .distinct()
                .collectMultimap(Player::getNational);
    }


}
