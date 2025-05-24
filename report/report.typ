#set text(font: "IBM Plex Sans", lang: "pt", region: "pt")
#show link: underline
#show link: text.with(fill: blue)

#set raw(lang: "rs")

#let name_and_number(name, number) = grid(
  row-gutter: 0.5em,
  text(weight: "medium", name), text(size: 1em, number),
)

#align(
  center,
  grid(
    row-gutter: 2em,
    grid(
      row-gutter: 0.7em,
      text(size: 2em, weight: "bold")[Tolerância a Faltas],
      text(size: 1.2em)[Relatório Trabalho Prático]
    ),
    grid(
      columns: 3,
      column-gutter: 6em,
      name_and_number[Diogo Marques][PG55931],
      name_and_number[Francisco Ferreira][PG55942],
      name_and_number[Ivan Ribeiro][PG55950],
    )
  ),
)

#v(2em)

= Introdução

Neste trabalho prático, pretendemos identificar e analisar possíveis vulnerabilidades do algoritmo Raft perante
a falhas assertivas que comprometam as propriedades de segurança do mesmo.

= Implementação do Raft

O algoritmo do Raft foi implementado em Rust.

A estrutura principal do código está dividida em módulos que separam as responsabilidades:
- `raft.rs`: Contém a lógica central do algoritmo Raft, incluindo a máquina de estados dos nodos (Seguidor, Candidato, Líder), a replicação de _logs_, e os processos de eleição.
- `maelstrom.rs`: Providencia a abstração para interagir com o ambiente _Maelstrom_, tratando da serialização/desserialização de mensagens e da comunicação entre nodos (ler do _stdin_ e escrever para o _stdout_).
- `transport.rs`: Define uma interface de transporte para o Raft (`RaftTransport`) e uma implementação específica para Maelstrom (`MaelstromTransport`), desacoplando a lógica do Raft do mecanismo de transporte subjacente.
- `main.rs`: Inicializa o nodo Maelstrom, integrando o sistema com os testes `lin-kv` do Maelstrom.

Para testar a sua implementação via _Maelstrom_ foi utilizado o seguinte comando:

```bash
cargo build --release
java -jar maelstrom.jar test
    -w lin-kv
    --bin ".\target\release\pessi-raft.exe"
    --time-limit 60
    --node-count 3
    --concurrency 10n
    --rate 100
    --nemesis partition
    --nemesis-interval 3
```

Como resultado, conseguimos observar que o _Maelstrom_ não detectou falhas no nosso algoritmo.

```
> Everything looks good!
```

No entanto, não é certo que o algoritmo esteja totalmente correto, pois o Maelstrom apenas testa um subconjunto limitado de cenários e não garante a cobertura de todos os possíveis estados ou falhas do sistema.

#pagebreak()

#show "Stateright": link("https://github.com/stateright/stateright")[_Stateright_]

= Verificação de estados com o Stateright

Para além dos testes de integração com o Maelstrom, recorreu-se ao Stateright para uma verificação mais formal do algoritmo Raft implementado. O Stateright é uma ferramenta de _model checking_ para sistemas distribuídos escritos em Rust, que permite explorar exaustivamente os possíveis estados de um sistema e verificar se certas propriedades se mantêm, a partir de código.

== Funcionamento e Aplicação ao Raft
O _model checking_ com Stateright envolve a definição de:
+ Um *modelo* do sistema: Este modelo descreve os estados possíveis de cada nodo Raft (e.g., `current_term`, `voted_for`, `log`, `commit_index`, `role`) e as ações que podem transitar o sistema de um estado para outro (e.g., enviar/receber uma mensagem `RequestVote`, `AppendEntries`, dar _timeout_ numa eleição);
+ Um *ambiente*: Define como as ações dos nodos interagem, incluindo a rede (que pode perder, duplicar ou reordenar mensagens) e outros eventos não determinísticos (mensagens que podem ser enviadas em qualquer momento);
+ *Propriedades*: São invariantes ou condições que devem ser sempre verdadeiras em todos os estados alcançáveis do sistema.

Esta definição foi feita através de:
- implementação do Raft, como modelo;
- ambiente com uma rede que não perde, duplica ou reordena mensagens;
- uma única mensagem, que consiste em escrever o valor `42` na chave `1`; #footnote[Esta mensagem foi escolhida de forma arbitrária e única para simplificar e acelerar a exploração dos possíveis estados do sistema, sendo suficiente para verificar todas as propriedades.]
- uma lista de propriedades que iremos apresentar a seguir;
- resto dependente do teste a fazer.

== Propriedades Verificadas
Foram definidas e verificadas várias propriedades fundamentais do Raft para garantir a sua correção:

+ *Segurança da Eleição (Election Safety)*<election_safety>: No máximo um líder pode ser eleito num determinado termo.
  - Expectativa temporal #footnote[`Always` significa que a propriedade deve ser respeitada em todos os estados alcançáveis do sistema, enquanto que `Sometimes` significa que a propriedade deve ser respeitada em alguns estados alcançáveis do sistema.]: `Always`

+ *Coerência do Log (Log Safety)*<log_safety>: Se dois _logs_ contêm uma entrada _committed_ com o mesmo índice e termo, então os _logs_ são idênticos até esse índice.
  - Expectativa temporal: `Always`

+ *Vivacidade do Log (Log Liveness)*<log_liveness>: O _log_ deverá progredir, isto é, haver um líder que aplique entradas no _log_. O _log_ não deverá ficar vazio eventualmente.
  - Expectativa temporal: `Sometimes`

+ *Vivacidade de Eleições (Election Liveness)*<election_liveness>: O sistema deverá progredir de modo a que um líder seja eleito. Um líder deverá ser eleito eventualmente.
  - Expectativa temporal: `Sometimes`

Estas propriedades, que se encontram em `src/fault/property.rs`, foram verificadas com o Stateright para o algoritmo Raft implementado, com sucesso.

= Faltas

Para analisar o comportamento do Raft perante falhas bizantinas e comportamentos maliciosos, criámos uma "API de Faltas" que permite injetar código em pontos críticos da execução. Pontos críticos são por exemplo, numa receção de mensagem, numa transição de estado importante (exemplo: líder eleito), etc. Desta forma, conseguimos simular anomalias sem alterar o código do algoritmo Raft diretamente.

O impacto de cada falha foi avaliado com testes unitários (usando `cargo test`) com a integração com o Stateright, permitindo verificar se as propriedades eram violadas sob essas condições, e se a mitigação de cada falha era eficaz.

Nas secções seguintes, detalhamos as faltas específicas investigadas, o seu impacto potencial no sistema, a demonstração da sua ocorrência, as possíveis medidas de mitigação e a demonstração dessas medidas.


#let fault_counter = counter("fault")

#let fault_subchapter_text_content(title, content) = grid(
  row-gutter: 0.6em,
  text(size: 1.1em, weight: "semibold", title),
  text(content),
)

#let alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

#let fault_chapter(
  title,
  identification,
  impact,
  demonstration,
  mitigation,
  mitigation_demonstration,
  title_label: "",
) = grid(
  row-gutter: 1.1em,
  [#heading(numbering: (_, i) => [Falta #alphabet.at(i - 1)], level: 2, [-] + title) #label(title_label)],
  fault_subchapter_text_content([Identificação da Falta], identification),
  fault_subchapter_text_content([Impacto], impact),
  fault_subchapter_text_content([Demonstração do Impacto], demonstration),
  fault_subchapter_text_content([Medidas de Mitigação], mitigation),
  fault_subchapter_text_content([Demonstração das Medidas de Mitigação], mitigation_demonstration),
)

#let demonstration_box(content) = block(
  inset: 1em,
  fill: luma(240),
  radius: 6pt,
  stroke: luma(200),
  content,
)

#let demonstration_subtitle = text.with(size: 1.1em, weight: "semibold")

#fault_chapter(title_label: "falsificacao")[
  Falsificação/Adulteração de Mensagens
][
  Um nodo malicioso pode falsificar ou adulterar (via _man-in-the-middle_) mensagens de outros nodos.
][
  O impacto desta falha é extremamente grave, pois um nodo malicioso com capacidade de falsificar ou adulterar mensagens pode comprometer completamente a segurança e a confiabilidade do cluster. Entre os impactos possíveis, destacam-se:

  - Derrubar eleições legítimas, falsificando mensagens de votação para impedir a eleição de um líder correto;
  - Eleger-se a si próprio, ao manipular votos ou resultados de eleições;
  - Injetar entradas falsas no log, comprometendo a integridade dos dados replicados;
  - Causar divisões no cluster, enviando mensagens contraditórias para diferentes nodos, levando a estados inconsistentes;
  - Impedir a propagação de comandos legítimos, bloqueando ou adulterando mensagens de confirmação;

  Em suma, a falsificação de mensagens permite a um atacante assumir controlo total do sistema, violando todas as propriedades de segurança e disponibilidade do protocolo Raft.
][
  A demonstração de falsificação via Maelstrom é simples, visto que é possível colocar qualquer `"src"` na mensagem e o Maelstrom não valida a origem da mensagem. Fora do Maelstrom, num sistema com comunicação via IP, também é possível falsificar mensagens, por exemplo, com recurso a _IP Spoofing_.

  No Stateright, que foi onde testámos a falha, conseguimos facilmente encontrar um caso de teste que viola a propriedade de *Election Safety*.

  Ao começar uma eleição, o nodo malicioso envia mensagens de votação de todos os nodos para ele próprio, de forma a que ele seja eleito, sem quaisquer verificações de outros nodos.
  Desta forma, é possível eleger dois líderes diferentes num mesmo termo, o que viola a propriedade de *Election Safety*.

  #demonstration_box[
    *Localização:* src/fault/message_forgery.rs

    #demonstration_subtitle[Ambiente de Teste]
    - *Atores:*
      - Id(0) --- #text(fill: red)[*Malicioso*]
      - Id(1) --- #text(fill: green.darken(50%))[Seguro]
      - Id(2) --- #text(fill: green.darken(50%))[Seguro]

    #demonstration_subtitle[Sequência de Eventos]
    + `Timeout(Id(1), ElectionTimeout)`
    + `Id(1)` $->$ `Raft(VoteRequest(VoteRequestMessage { node_id: Id(1), current_term: 1, log_length: 0, last_log_term: 0 }))` $->$ `Id(2)`
    + `Id(2)` $->$ `Raft(VoteResponse(VoteResponseMessage { node_id: Id(2), current_term: 1, vote_granted: true }))` $->$ `Id(1)`
    + `Id(0)` $->$ `Raft(VoteResponse(VoteResponseMessage { node_id: Id(1), current_term: 1, vote_granted: true }))` $->$ `Id(0)`
    + `Timeout(Id(0), ElectionTimeout)`
    + `Id(0)` $->$ `Raft(VoteResponse(VoteResponseMessage { node_id: Id(1), current_term: 1, vote_granted: true }))` $->$ `Id(0)`

    Quando o nodo malicioso (0) inicia uma eleição, ele simula que recebeu um voto do nodo 1 (basta um para quórum), e assim é eleito como líder, mesmo que já exista outro líder no mesmo termo.

    #demonstration_subtitle[*Estado Inválido Encontrado*]
    #grid(
      columns: 3,
      column-gutter: 1fr,
      ```rs
        Node {
          id: Id(0),
          current_role: Leader,
          ...
        }
      ```,
      ```rs
      Node {
        id: Id(1),
        current_role: Leader,
        ...
      }
      ```,
      ```rs
      Node {
        id: Id(2),
        current_role: Follower,
        ...
      }
      ```,
    )

    #demonstration_subtitle[Propriedades Violadas]
    - #link(<election_safety>)[Segurança da Eleição (Election Safety)]
  ]
][
  Seria possível mitigar completamente este problema adicionando autenticação às mensagens, por exemplo, através de assinaturas digitais.
][
  Devido à natureza do problema, que não é relacionado com os conteúdos abordados na unidade curricular, não iremos implementar a mitigação.
]

#fault_chapter[
  Voto Duplo
][
  Um nodo malicioso pode votar em dois candidatos diferentes numa eleição no mesmo termo.
][
  Permite que dois nodos sejam eleitos como líder, o que quebra a propriedade de *Election Safety* do algoritmo.
][
  Com o Stateright, podemos injetar código na receção do `VoteRequest` e responder sempre positivamente ao voto, sem verificar se o nodo já votou noutro candidato.

  Mais concretamente, o nodo malicioso enviará sempre a mensagem `VoteResponse` com `vote_granted` a #raw(lang: "rs", "true"), independentemente do voto que recebeu.

  #demonstration_box[
    *Localização:* src/fault/double_vote.rs

    #demonstration_subtitle[Ambiente de Teste]
    - *Atores:*
      - Id(0) --- #text(fill: red)[*Malicioso*]
      - Id(1) --- #text(fill: green.darken(50%))[Seguro]
      - Id(2) --- #text(fill: green.darken(50%))[Seguro]

    #demonstration_subtitle[Sequência de Eventos]
    + `Timeout(Id(0), ElectionTimeout)`
    + `Timeout(Id(2), ElectionTimeout)`
    + `Id(0)` $->$ `Raft(VoteRequest(VoteRequestMessage { node_id: Id(0), current_term: 1, log_length: 0, last_log_term: 0 }))` $->$ `Id(1)`
    + `Id(0)` $->$ `Raft(VoteRequest(VoteRequestMessage { node_id: Id(0), current_term: 1, log_length: 0, last_log_term: 0 }))` $->$ `Id(2)`
    + `Id(2)` $->$ `Raft(VoteRequest(VoteRequestMessage { node_id: Id(2), current_term: 1, log_length: 0, last_log_term: 0 }))` $->$ `Id(0)`
    + `Id(0)` $->$ `Raft(VoteResponse(VoteResponseMessage { node_id: Id(0), current_term: 1, vote_granted: true }))` $->$ `Id(2)`
    + `Id(1)` $->$ `Raft(VoteResponse(VoteResponseMessage { node_id: Id(1), current_term: 1, vote_granted: true }))` $->$ `Id(0)`

    Podemos observar que o nodo malicioso começa a atuar no evento nº6, que deveria de responder ao voto do nodo 2, como `vote_granted: false`, já que tem uma eleição em curso com o `log_length` maior ou igual ao do nodo 2, mas responde `vote_granted: true`. Com o nodo 1 a também responder `vote_granted: true` ao nodo 0, o nodo 0 também é eleito como líder.

    #demonstration_subtitle[*Estado Inválido Encontrado*]
    #grid(
      columns: 3,
      column-gutter: 1fr,
      ```rs
      Node {
        id: Id(0),
        current_role: Leader,
        ...
      }
      ```,
      ```rs
      Node {
        id: Id(1),
        current_role: Follower,
        ...
      }
      ```,
      ```rs
      Node {
        id: Id(2),
        current_role: Leader,
        ...
      }
      ```,
    )

    #demonstration_subtitle[Propriedades Violadas]
    - #link(<election_safety>)[Segurança da Eleição (Election Safety)]
  ]

][
  É possível resolver este problema com uma mensagem adicional, por exemplo, `ElectedBy`, que é enviada num fim de eleição, por todos os nodos que foram eleitos para todos os outros nodos, com a lista de nodos que votaram nele. Caso seja detetado um nodo que votou em dois candidatos diferentes, outros nodos podem ignorar o seu voto futuramente (_black-listed_).

  Daqui surge um novo problema, que é, um nodo bizantino pode dizer que recebeu votos de quem não votou nele, o que faria que esse outro nodo fosse _black-listed_ indevidamente. Uma solução possível a este problema é recorrer a assinaturas digitais, que permitiriam identificar a autenticidade do voto.
][
  Como a segunda parte desta solução não é abordada na unidade curricular, não iremos implementar a mitigação com assinaturas digitais.

  No entanto, a primeira parte da solução pode ser implementada, e é o que faremos.

  #demonstration_box[
    *Localização:* src/fault/double_vote_fix.rs

    #demonstration_subtitle[Ambiente de Teste]
    - *Atores:*
      - Id(0) --- #text(fill: red)[*Malicioso*]
      - Id(1) --- #text(fill: blue.darken(50%))[Seguro com mitigação]
      - Id(2) --- #text(fill: blue.darken(50%))[Seguro com mitigação]
    - *Propriedades a verificar:*
      - O nodo malicioso é #text(fill: red)[black-listed];
        - Expectativa temporal: `Sometimes`

    Não conseguimos garantir todas as propriedades de segurança imediatamente após a eleição, porque o nodo malicioso ainda pode causar a existência de dois líderes ao mesmo tempo.

    Isto acontece porque a mensagem `ElectedBy`, que permite detetar votos duplicados, só é enviada no final da eleição. Assim, até que essa mensagem seja processada e o nodo malicioso seja identificado e colocado na lista negra (_black-listed_), ainda pode haver uma violação temporária da propriedade de "Election Safety".

    No entanto, após o nodo malicioso ser _black-listed_, é invocada uma eleição imediatamente, e os seus votos passam a ser ignorados nas eleições seguintes, restaurando o funcionamento correto do algoritmo e prevenindo futuras violações desta propriedade.

    Apenas precisamos garantir que o nodo malicioso é colocado na lista negra (_black-listed_). As restantes propriedades já foram validadas em cenários sem nodos maliciosos, e, após o _black-list_, o sistema volta a funcionar como se não existissem nodos maliciosos.

    #demonstration_subtitle[Sequência de Eventos]
    + `Timeout(Id(2), ElectionTimeout)`
    + `Id(2)` $->$ `Raft(VoteRequest(VoteRequestMessage { node_id: Id(2), current_term: 1, log_length: 0, last_log_term: 0 }))` $->$ `Id(0)`
    + `Timeout(Id(1), ElectionTimeout)`
    + `Id(1)` $->$ `Raft(VoteRequest(VoteRequestMessage { node_id: Id(1), current_term: 1, log_length: 0, last_log_term: 0 }))` $->$ `Id(0)`
    + `Id(2)` $->$ `Raft(VoteRequest(VoteRequestMessage { node_id: Id(2), current_term: 1, log_length: 0, last_log_term: 0 }))` $->$ `Id(1)`
    + `Id(0)` $->$ `Raft(VoteResponse(VoteResponseMessage { node_id: Id(0), current_term: 1, vote_granted: true }))` $->$ `Id(1)`
    + `Id(0)` $->$ `Raft(VoteResponse(VoteResponseMessage { node_id: Id(0), current_term: 1, vote_granted: true }))` $->$ `Id(2)`
    + `Id(1)` $->$ `Other(ElectedBy { leader: Id(1), term: 1, by: [Id(1), Id(0)] })` $->$ `Id(1)`
    + `Id(2)` $->$ `Other(ElectedBy { leader: Id(2), term: 1, by: [Id(2), Id(0)] })` $->$ `Id(1)`

    O nodo malicioso (0) viola o protocolo ao conceder voto positivo a dois candidatos diferentes (1 e 2) durante o mesmo termo de eleição. Como resultado, ambos os nodos 1 e 2 acreditam ter obtido a maioria dos votos e assumem simultaneamente o papel de líder, o que viola a propriedade de "Election Safety" do algoritmo Raft. Esta situação só é detetada após o envio das mensagens `ElectedBy`, quando os nodos honestos percebem que o mesmo nodo (0) votou em mais do que um candidato. A partir desse momento, o nodo malicioso é colocado na lista negra (_black-listed_) e os seus votos deixam de ser considerados em eleições futuras, restaurando a segurança do sistema.

    Neste caso, apenas o nodo 1 detetou inicialmente o problema; no entanto, em eventos subsequentes, o nodo 2 também irá identificar a infração.

    #demonstration_subtitle[*Estado Inválido Encontrado*]
    #grid(
      columns: 3,
      column-gutter: 1fr,
      ```rs
      Node {
        id: Id(0),
        blacklist: [],
        current_role: Follower,
        ...
      }
      ```,
      ```rs
      Node {
        id: Id(1),
        blacklist: [Id(0)],
        current_role: Follower,
        ...
      }
      ```,
      ```rs
      Node {
        id: Id(2),
        blacklist: [],
        current_role: Leader,
        ...
      }
      ```,
    )

    #demonstration_subtitle[Propriedades Verificadas]
    - Nodo Malicioso é #text(fill: red)[black-listed];
  ]
]

#fault_chapter[
  Negação de Serviço por Spam de Eleições
][
  Cada _follower_ tem uma _timer_ para dar _timeout_ e começar uma eleição, quando não recebe atualizações de um líder. Um nodo malicioso pode simplesmente ignorar esse _timer_ e começar sempre uma nova eleição. A cada momento desses, o nodo malicioso, incrementa o seu _term_ e tenta eleger-se como líder. Como o seu _term_ é maior que o do líder, todos os nodos (incluindo o líder) votam nele, e ele passa a ser o líder.

  Este nodo malicioso pode continuar a fazer isto indefinidamente, sempre que um líder é eleito.
][
  Como não há um líder estável, a propriedade de _liveness_ não é respeitada, isto é, o sistema não consegue fazer progresso, visto que não há tempo suficiente para replicar as entradas no log.
][
  Com o Stateright, podemos fazer com que a cada mensagem recebida, o nodo malicioso comece uma eleição.

  #demonstration_box[
    *Localização:* src/fault/election_spam.rs

    #demonstration_subtitle[Ambiente de Teste]
    - *Atores:*
      - Id(0) --- #text(fill: red)[*Malicioso*]
      - Id(1) --- #text(fill: red)[*Malicioso*]
      - Id(2) --- #text(fill: green.darken(50%))[Seguro]

    Inicialmente, não tínhamos considerado que, com apenas um nodo malicioso, o Stateright poderia encontrar cenários em que todas as propriedades de segurança fossem satisfeitas. Isto acontece porque, se o nodo malicioso atrasar o envio das mensagens, os outros dois nodos ainda conseguem formar uma maioria e executar o algoritmo Raft corretamente.

    Por isso, foi necessário ajustar o teste para incluir dois nodos maliciosos. Assim, garantimos que há sempre eleições a decorrer, impedindo a verificação da propriedade de _Log Liveness_.

    Ainda assim, mesmo com dois nodos maliciosos, o Stateright encontrou uma possibilidade de ainda haver líder na implementação do spam.
    Se um dos nodos maliciosos começar uma eleição e o nodo 2 responder positivamente, o nodo que começou a eleição passa a ser o líder.

    #demonstration_subtitle[Propriedades Não Verificadas]
    - #link(<log_liveness>)[Vivacidade do Log (Log Liveness)]

  ]
][
  Para este problema, não há uma solução concreta, e teremos que recorrer a soluções heurísticas.

  Uma solução possível é ignorar começos de eleições demasiado cedo, por exemplo, se um _term_ começou há menos de 1 minuto, ignorar o começo dele. No entanto, esta solução pode fazer com que o sistema volte demasiado tempo a voltar ao normal caso o líder realmente morra nesse espaço de tempo.

  Outra solução possível é limitar o número de eleições que um nodo pode iniciar num dado espaço de tempo, por exemplo, 1 eleição por hora.
][
  Optámos por uma abordagem mais simples: se um nodo iniciar eleições em três ocasiões consecutivas, esse nodo é colocado na lista negra (_black-listed_). Desta forma, previne-se que nodos maliciosos possam continuamente desestabilizar o sistema através de spam de eleições.

  #demonstration_box[
    *Localização:* src/fault/election_spam_fix.rs

    #demonstration_subtitle[Ambiente de Teste]
    - *Atores:*
      - Id(0) --- #text(fill: red)[*Malicioso*]
      - Id(1) --- #text(fill: blue.darken(50%))[Seguro com mitigação]
      - Id(2) --- #text(fill: blue.darken(50%))[Seguro com mitigação]

    - *Propriedades a verificar:*
      - #link(<election_safety>)[Segurança da Eleição (Election Safety)]
      - #link(<log_liveness>)[Vivacidade do Log (Log Liveness)]
      - #link(<election_liveness>)[Vivacidade de Eleições (Election Liveness)]
      - #link(<election_safety>)[Segurança da Eleição (Election Safety)]
      - O nodo malicioso é #text(fill: red)[black-listed];
        - Expectativa temporal: `Sometimes`

    Todas as propriedades de segurança foram verificadas com sucesso. No entanto, apenas demonstraremos a sequência de eventos que levaram à verificação do nodo malicioso estar na lista negra (_black-listed_).

    #demonstration_subtitle[Sequência de Eventos]
    + `Timeout(Id(0), ElectionTimeout)`
    + `Id(0)` $->$ `Raft(VoteRequest(VoteRequestMessage { node_id: Id(0), current_term: 1, log_length: 0, last_log_term: 0 }))` $->$ `Id(2)`
    + `Id(2)` $->$ `Raft(VoteResponse(VoteResponseMessage { node_id: Id(2), current_term: 1, vote_granted: true }))` $->$ `Id(0)`
    + `Id(0)` $->$ `Raft(VoteRequest(VoteRequestMessage { node_id: Id(0), current_term: 2, log_length: 0, last_log_term: 0 }))` $->$ `Id(2)`
    + `Id(2)` $->$ `Raft(VoteResponse(VoteResponseMessage { node_id: Id(2), current_term: 2, vote_granted: true }))` $->$ `Id(0)`
    + `Id(0)` $->$ `Raft(VoteRequest(VoteRequestMessage { node_id: Id(0), current_term: 3, log_length: 0, last_log_term: 0 }))` $->$ `Id(2)`

    Neste cenário, o nodo malicioso (0) inicia três eleições consecutivas, sendo que o nodo 2 recebe todos esses pedidos. Após a terceira tentativa, o nodo 0 é colocado na lista negra (_black-listed_) do nodo 2, impedindo-o de continuar a desestabilizar o sistema.

    #demonstration_subtitle[*Estado Válido Encontrado*]
    #grid(
      columns: 3,
      column-gutter: 1fr,
      ```rs
      Node {
        id: Id(0),
        blacklist: [],
        current_role: Candidate,
        ...
      }
      ```,
      ```rs
      Node {
        id: Id(1),
        blacklist: [],
        current_role: Follower,
        ...
      }
      ```,
      ```rs
      Node {
        id: Id(2),
        blacklist: [Id(0)],
        current_role: Follower,
        ...
      }
      ```,
    )

    O resto das propriedades podem ser verificadas rodando o teste unitário que executa o Stateright, presente em src/fault/election_spam_fix.rs.

    #demonstration_subtitle[Propriedades Verificadas]
    - Nodo Malicioso é #text(fill: red)[black-listed];
    - #link(<election_safety>)[Segurança da Eleição (Election Safety)]
    - #link(<election_liveness>)[Vivacidade de Eleições (Election Liveness)]
    - #link(<election_safety>)[Segurança da Eleição (Election Safety)]
    - #link(<log_liveness>)[Vivacidade do Log (Log Liveness)]
  ]
]

#fault_chapter[
  Bifurcação de _log_
][
  Um nodo malicioso pode enviar, no mesmo índice e termo, duas versões diferentes do _log_ para _subsets_ distintos de nodos. Com isto, diferentes _quorum's_ de nodos recebem versões diferentes do _log_, o que faz com que o _log_ se bifurque.
][
  Se dois conjuntos de nodos aplicarem entradas diferentes no mesmo índice, a propriedade de _*Log Safety*_ é violada: leituras ou escritas podem comportar-se de forma inconsistente em caso de falhas do líder.
][
  Para demonstrar essa vulnerabilidade, podemos considerar um cenário em que um nodo malicioso, a cada `LogRequest`, incrementa todos os valores das entradas do _log_ em uma unidade antes de enviá-las. Esse comportamento resulta em divergência entre as entradas _committed_ dos diferentes nodos, comprometendo a consistência do sistema.

  #demonstration_box[
    *Localização:* src/fault/forking_log.rs

    #demonstration_subtitle[Ambiente de Teste]
    - *Atores:*
      - Id(0) --- #text(fill: red)[*Malicioso*]
      - Id(1) --- #text(fill: green.darken(50%))[Seguro]
      - Id(2) --- #text(fill: green.darken(50%))[Seguro]

    #demonstration_subtitle[*Sequência de Eventos*]
    + `Timeout(Id(1), ElectionTimeout)`
    + `Id(1)` $->$ `Raft(VoteRequest(VoteRequestMessage { node_id: Id(1), current_term: 1, log_length: 0, last_log_term: 0 }))` $->$ `Id(0)`
    + `Id(0)` $->$ `Raft(VoteResponse(VoteResponseMessage { node_id: Id(0), current_term: 1, vote_granted: true }))` $->$ `Id(1)`
    + `Id(1)` $->$ `Raft(LogRequest(LogRequestMessage { leader_id: Id(1), current_term: 1, prefix_length: 0, prefix_term: 0, commit_length: 0, suffix: [] }))` $->$ `Id(0)`
    + `Id(0)` $->$ `Raft(LogResponse(LogResponseMessage { node_id: Id(0), current_term: 1, ack: 0, success: true }))` $->$ `Id(1)`
    + `Id(1)` $->$ `ClientRequest(Write { key: 1, value: 42, msg_id: 1 })` $->$ `Id(1)`
    + `Id(1)` $->$ `Raft(LogRequest(LogRequestMessage { leader_id: Id(1), current_term: 1, prefix_length: 0, prefix_term: 0, commit_length: 0, suffix: [LogEntry { term: 1, from: Id(1), request: Write { key: 1, value: 42, msg_id: 1 } }] }))` $->$ `Id(0)`
    + `Id(0)` $->$ `Raft(LogResponse(LogResponseMessage { node_id: Id(0), current_term: 1, ack: 1, success: true }))` $->$ `Id(1)`

    Observa-se que, no evento nº0, o nodo 1 é eleito como líder. Já no evento nº7, o nodo 0 — que é malicioso — recebe a entrada do log, incrementa o valor e, assim, introduz uma inconsistência no log. Posteriormente, no próximo evento, ao receber a confirmação do nodo 0, o nodo 1 realiza o _commit_ dessa entrada adulterada, consolidando a inconsistência no sistema.

    #demonstration_subtitle[*Estado Inválido Encontrado*]
    #grid(
      columns: 3,
      column-gutter: 1fr,
      ```rs
      Node {
        id: Id(0),
        log: [
          LogEntry {
            term: 1,
            from: Id(1),
            request: Write {
              key: 1,
              value: 43,
              msg_id: 1,
            },
          },
        ],
        commit_length: 0,
        current_role: Follower,
        ...
      }
      ```,
      ```rs
      Node {
        id: Id(1),
        log: [
          LogEntry {
            term: 1,
            from: Id(1),
            request: Write {
              key: 1,
              value: 42,
              msg_id: 1,
            },
          },
        ],
        commit_length: 1,
        current_role: Leader,
        ...
      }
      ```,
      ```rs
      Node {
        id: Id(2),
        log: [],
        commit_length: 0,
        current_role: Follower,
        ...
      }
      ```,
    )

    #demonstration_subtitle[Propriedades Não Verificadas]
    - #link(<log_safety>)[Segurança do Log (Log Safety)]
  ]

][
  Uma possível solução para esta falta, seria aquando a receção de um `LogRequest` contendo novas entradas do _log_, enviar para todos os outros nodos, uma mensagem adicional que contivesse essas novas entradas. Desta forma, os outros nodos veriam que o _log_ não é consistente e começariam uma nova eleição, bloqueando o nodo malicioso.

  Para evitar grande tráfego de mensagens, podemos aliviar o envio dessa mensagem adicional para apenas alguns nodos e periodicamente, e não a cada `LogRequest`, como é no protocolo Gossip.
][
  #text(fill: red)[A FAZER]
]

#fault_chapter[
  _Commit_ de _log_ sem ser aceite pela maioria
][
  Um nodo líder malicioso pode incrementar o `commit_index` sem ter recebido confirmação da maioria dos nodos.
][
  Os _logs_ podem não estar atualizados numa maioria dos nodos, o que faria com que caso o líder falhasse, dados seriam perdidos. Quebra o princípio de _safety_.
][
  Com o Stateright, podemos criar um nodo malicioso que incrementa o `commit_index` para o tamanho do _log_ depois de qualquer mensagem ter sido enviada.

  #demonstration_box[
    *Localização:* src/fault/fake_commit_log.rs

    #demonstration_subtitle[Ambiente de Teste]
    - *Atores:*
      - Id(0) --- #text(fill: red)[*Malicioso*]
      - Id(1) --- #text(fill: green.darken(50%))[Seguro]
      - Id(2) --- #text(fill: green.darken(50%))[Seguro]

    Neste caso, embora o _Stateright_ suporte falhas por _crash_, ele não permite que nodos revivam nem simula partições de rede entre os atores. Por esse motivo, não é possível testar cenários em que quando o `commit_index` é incrementado de forma maliciosa, leva a que diferentes nodos tenham entradas _committed_ divergentes no _log_, já que não ocorre divergência real de entradas entre os nodos durante a execução.

    #text(fill: red)[A EXPLORAR MELHOR]
  ]
][
  Este problema poderia ser resolvido, enviando uma assinatura digital como forma de autenticidade e autenticação em cada `LogResponse`. Assim, o líder era obrigado a guardar essas assinaturas, para as propagar a seguir. Os outros nodos só aceitariam `LogRequest`'s que incrementem o `commit_index`, se nela conter assinaturas digitais válidas de um quorum de nodos.
][
  Da mesma forma que na solução da @falsificacao[], devido à natureza do problema, que não está relacionada com os conteúdos abordados na unidade curricular, não iremos implementar a mitigação.
]
