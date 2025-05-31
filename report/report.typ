#set text(font: "IBM Plex Sans", lang: "pt", region: "pt")
#set par(justify: true)

#set page(numbering: "1/1")

#show link: underline
#show link: text.with(fill: blue)

#set raw(lang: "rs")

#let name_and_number(name, number) = grid(
  row-gutter: 0.5em,
  text(weight: "medium", name), text(size: 1em, number),
)

#show raw.where(block: true): block.with(
  width: 100%,
  inset: 0.80em,
  radius: 6pt,
  fill: luma(240),
  stroke: luma(200),
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

#show "Stateright": link("https://github.com/stateright/stateright")[_Stateright_]

= Introdução

Neste trabalho prático, pretendemos identificar e analisar possíveis vulnerabilidades do algoritmo de _Raft_ perante falhas assertivas que comprometam as propriedades de segurança do mesmo.

Inicialmente, descrevemos a nossa implementação do _Raft_ e a sua integração com o ambiente _Maelstrom_, de seguida o uso do Stateright para verificar a correção do algoritmo face a possíveis falhas e respetivas mitigações.

= Implementação do Raft

O algoritmo de Raft foi implementado na linguagem de programação _Rust_. Quando o programa é executado, é inicializado um nó _Maelstrom_ que aguarda mensagens do _stdin_ e escreve mensagens para o _stdout_.

A estrutura principal do código está dividida em módulos que separam as responsabilidades:

- *`raft.rs`*: Contém a lógica central do algoritmo de _Raft_, incluindo a máquina de estados dos nós (Seguidor, Candidato, Líder), a replicação de _logs_ e os processos de eleição.

- *`maelstrom.rs`*: Providencia a abstração para interagir com o ambiente _Maelstrom_, tratando da serialização/desserialização de mensagens e da comunicação entre nós (ler do _stdin_ e escrever para o _stdout_).

- *`transport.rs`*: Define uma interface de transporte para o _Raft_ (`RaftTransport`) e uma implementação específica para _Maelstrom_ (`MaelstromTransport`), desacoplando a lógica do mecanismo de transporte subjacente.

- *`main.rs`*: Inicializa o nó _Maelstrom_, integrando o sistema com os testes `lin-kv` do _Maelstrom_.

Para testar a implementação via _Maelstrom_ foi utilizado o seguinte comando:

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

Como resultado, observamos que o _Maelstrom_ não detetou falhas no nosso algoritmo.

```
> Everything looks good!
```

No entanto, não é certo que o algoritmo esteja totalmente correto, pois o _Maelstrom_ apenas testa um subconjunto limitado de cenários e não garante a cobertura de todos os estados possíveis ou falhas do sistema.

= Verificação de Estados com o Stateright

Para além dos testes de integração com o _Maelstrom_, recorreu-se ao Stateright para uma verificação mais formal do algoritmo de _Raft_ implementado. O Stateright é uma ferramenta de _model checking_ para sistemas distribuídos escritos em _Rust_, que permite explorar exaustivamente os possíveis estados de um sistema e verificar se certas propriedades se mantêm, tudo a partir de código.

== Funcionamento e Aplicação ao Raft
O _model checking_ com Stateright envolve a definição de:

+ *Um modelo do sistema*: Este modelo descreve os estados possíveis de cada nó (e.g., `current_term`, `voted_for`, `log`, `commit_index`, `role`) e as ações que podem transitar o sistema de um estado para outro (e.g., enviar/receber uma mensagem `RequestVote`, `AppendEntries`, dar _timeout_ numa eleição);

+ *Um ambiente*: Define como as ações dos nós interagem, incluindo a rede (que pode perder, duplicar ou reordenar mensagens) e outros eventos não determinísticos (mensagens que podem ser enviadas em qualquer momento);

+ *Propriedades*: São invariantes ou condições que devem ser sempre verdadeiras em todos os estados alcançáveis do sistema.

Esta definição foi realizada através de:

- Implementação do _Raft_, como modelo;
- Ambiente com uma rede que não perde, duplica ou reordena mensagens;
- Uma única mensagem, que consiste em escrever o valor `42` na chave `1`; #footnote[Esta mensagem foi escolhida de forma arbitrária e única para simplificar e acelerar a exploração dos possíveis estados do sistema, sendo suficiente para verificar todas as propriedades.]
- Uma lista de propriedades que iremos apresentar a seguir;
- Resto dependente do teste a fazer.

== Propriedades Verificadas
Foram definidas e verificadas propriedades essenciais do Raft para garantir a sua correção:

+ *Segurança da Eleição (Election Safety)*<election_safety>: No máximo um líder pode ser eleito num determinado termo.
  - Expectativa temporal #footnote[`Always` significa que a propriedade deve ser respeitada em todos os estados alcançáveis do sistema, enquanto que `Sometimes` significa que a propriedade deve ser respeitada em alguns estados alcançáveis do sistema.]: `Always`

+ *Coerência do Log (Log Safety)*<log_safety>: Se dois _logs_ contêm uma entrada _committed_ com o mesmo índice e termo, então os _logs_ são idênticos até esse índice.
  - Expectativa temporal: `Always`

+ *Vivacidade do Log (Log Liveness)*<log_liveness>: O _log_ deverá progredir, isto é, haver um líder que aplique entradas no _log_. O _log_ não deverá ficar vazio eternamente.
  - Expectativa temporal: `Sometimes`

+ *Vivacidade de Eleições (Election Liveness)*<election_liveness>: O sistema deverá progredir de modo a que um líder seja eleito. Um líder deverá ser eleito inevitavelmente.
  - Expectativa temporal: `Sometimes`

Estas propriedades, que se encontram em `src/fault/property.rs`, foram afortunadamente verificadas com o Stateright para o algoritmo de _Raft_ implementado.

A implementação do nosso algoritmo de _Raft_ é feita sobre uma abstração de transporte, o que nos permite facilmente criar um transporte específico para o Stateright sem modificar o código do algoritmo. Assim, conseguimos executar o _Raft_ diretamente sobre este ambiente, facilitando a verificação formal das respetivas propriedades.

#pagebreak()

= Faltas

Para analisar o comportamento do _Raft_ perante falhas bizantinas e comportamentos maliciosos, criámos uma "API de Faltas" que permite injetar código em pontos críticos da execução. Pontos críticos são por exemplo, numa receção de mensagem, numa transição de estado importante (exemplo: líder eleito), etc. Desta forma, conseguimos simular anomalias sem alterar o código do algoritmo diretamente.

O impacto de cada falha foi avaliado a partir de testes unitários (usando `cargo test`) com a integração do Stateright, permitindo verificar se as propriedades eram violadas sob essas condições, e se a mitigação de cada falha era eficaz.

Nas secções seguintes, detalhamos as faltas investigadas, o seu impacto potencial no sistema, a evidência de ocorrências, as possíveis medidas de mitigação e demonstração das mesmas.


#let fault_counter = counter("fault")

#let fault_subchapter_text_content(title, content) = grid(
  row-gutter: 0.6em,
  if content != [] { text(size: 1.1em, weight: "semibold", title) },
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
  Um nó malicioso pode falsificar ou adulterar mensagens de outros nós.
][
  O impacto desta falha é extremamente grave, pois um nó malicioso com capacidade de falsificar ou adulterar mensagens pode comprometer completamente a segurança e a confiabilidade do _cluster_. Entre os impactos possíveis, destacam-se:

  - Derrubar eleições legítimas, falsificando mensagens de votação para impedir a eleição de um líder correto;

  - Eleger-se a si próprio, ao manipular votos ou resultados de eleições;

  - Injetar entradas falsas no _log_, comprometendo a integridade dos dados replicados;

  - Causar divisões no _cluster_, enviando mensagens contraditórias para diferentes nós, levando a estados inconsistentes;

  - Impedir a propagação de comandos legítimos, bloqueando ou adulterando mensagens de confirmação;

  Em suma, a falsificação de mensagens permite a um atacante assumir controlo total do sistema, violando todas as propriedades de segurança e disponibilidade do protocolo _Raft_.
][
  A demonstração de falsificação pode ser feita alterando os campos onde é enviado o `node_id` da mensagem. A identificação do nó é feita em campos de mensagens do _Raft_, e estes não são validados.

  ```rs
  struct VoteRequestMessage {
      node_id: Id, /// <--- Identificação do nó que envia a mensagem
      current_term: TermId,
      log_length: usize,
      last_log_term: TermId,
  }
  ```

  No Stateright, que foi onde testámos a falha, conseguimos facilmente encontrar um caso de teste que viola a propriedade de *Election Safety*.

  Ao começar uma eleição, o nó malicioso envia mensagens de votação de todos os nós para ele próprio, de forma a que ele seja eleito, sem quaisquer verificações de outros nós. Desta forma, é possível eleger dois líderes diferentes num mesmo termo, o que viola a propriedade de *Election Safety*.

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

    Quando o nó malicioso (0) inicia uma eleição, ele simula que recebeu um voto do nó 1 (basta um para quórum), e assim é eleito como líder, mesmo que já exista outro líder no mesmo termo.

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

#pagebreak()

#fault_chapter[
  Voto Duplo
][
  Um nó malicioso pode votar em dois candidatos diferentes numa eleição no mesmo termo.
][
  Permite que dois nós sejam eleitos como líder, o que quebra a propriedade de *Election Safety* do algoritmo.
][
  Com o Stateright, podemos injetar código na receção do `VoteRequest` e responder sempre positivamente ao voto, sem verificar se o nó já votou noutro candidato. Mais concretamente, o nó malicioso enviará sempre a mensagem `VoteResponse` com `vote_granted` a #raw(lang: "rs", "true"), independentemente do pedido de voto que receber.

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

    Podemos observar que o nó malicioso começa a atuar no evento n.º6, que deveria de responder ao voto do nó 2, como `vote_granted: false`, já que tem uma eleição em curso com o `log_length` maior ou igual ao do nó 2, mas responde `vote_granted: true`. Com o nó 1 a também responder `vote_granted: true` ao nó 0, o nó 0 também é eleito como líder.

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
  É possível resolver este problema com uma mensagem adicional, por exemplo, `ElectedBy`, que é difundida no fim da eleição por todos os nós eleitos, possuindo esta a lista de nós votantes. Caso seja detetado um nó que votou em dois candidatos diferentes, outros nós podem ignorar o seu voto futuramente (_black-listed_).

  Daqui surge um novo problema, que é, um nó bizantino pode dizer que recebeu votos de quem não votou nele, o que faria que esse outro nó fosse _black-listed_ indevidamente. Uma solução possível a este problema é recorrer a assinaturas digitais, que permitiriam identificar a autenticidade do voto.
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
      - O nó malicioso é #text(fill: red)[black-listed];
        - Expectativa temporal: `Sometimes`

    Não conseguimos garantir todas as propriedades de segurança imediatamente após a eleição, porque o nó malicioso ainda pode causar a existência de dois líderes ao mesmo tempo.

    Isto acontece porque a mensagem `ElectedBy`, que permite detetar votos duplicados, só é enviada no final da eleição. Assim, até que essa mensagem seja processada e o nó malicioso seja identificado e colocado na lista negra (_black-listed_), ainda pode haver uma violação temporária da propriedade de "Election Safety".

    No entanto, após o nó malicioso ser _black-listed_, é invocada uma eleição imediatamente, e os seus votos passam a ser ignorados nas eleições seguintes, restaurando o funcionamento correto do algoritmo e prevenindo futuras violações desta propriedade.

    Apenas precisamos de garantir que o nó malicioso é colocado na lista negra (_black-listed_). As restantes propriedades já foram validadas em cenários sem nós maliciosos, e, após o _black-list_, o sistema volta a funcionar como se não existissem nós maliciosos.

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

    O nó malicioso (0) viola o protocolo ao conceder voto positivo a dois candidatos diferentes (1 e 2) durante o mesmo termo de eleição. Como resultado, os nós 1 e 2 acreditam ter obtido a maioria dos votos e assumem simultaneamente o papel de líder, o que viola a propriedade de "Election Safety" do algoritmo de _Raft_. Esta situação só é detetada após o envio das mensagens `ElectedBy`, quando os nós honestos percebem que o mesmo nó (0) votou em mais do que um candidato. A partir desse momento, o nó malicioso é colocado na lista negra (_black-listed_) e os seus votos deixam de ser considerados em eleições futuras, restaurando a segurança do sistema.

    Neste caso, apenas o nó 1 detetou inicialmente o problema; no entanto, em eventos subsequentes, o nó 2 também irá identificar a infração.

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
    - Nó Malicioso é #text(fill: red)[black-listed];
  ]
]

#pagebreak()

#fault_chapter[
  Negação de Serviço por Spam de Eleições
][
  Cada _follower_ tem um _timer_ para dar _timeout_ e começar uma eleição, quando não recebe atualizações de um líder. Um nó malicioso pode simplesmente ignorar esse _timer_ e começar sempre uma nova eleição. A cada momento desses, o nó malicioso, incrementa o seu _term_ e tenta eleger-se como líder. Como o seu _term_ é maior que o do líder, todos os nós (incluindo o líder) votam nele, e ele passa a ser o líder.

  Este nó malicioso pode continuar a fazer isto indefinidamente, sempre que um líder é eleito.
][
  Como não há um líder estável, a propriedade de _liveness_ não é respeitada, isto é, o sistema não consegue fazer progresso, visto que não há tempo suficiente para replicar as entradas no _log_.
][
  Com o Stateright, podemos fazer com que a cada mensagem recebida o nó malicioso comece uma eleição.

  #demonstration_box[
    *Localização:* src/fault/election_spam.rs

    #demonstration_subtitle[Ambiente de Teste]
    - *Atores:*
      - Id(0) --- #text(fill: red)[*Malicioso*]
      - Id(1) --- #text(fill: red)[*Malicioso*]
      - Id(2) --- #text(fill: green.darken(50%))[Seguro]

    Inicialmente, não tínhamos considerado que, com apenas um nó malicioso, o Stateright poderia encontrar cenários em que todas as propriedades de segurança fossem satisfeitas. Isto acontece porque, se o nó malicioso atrasar o envio das mensagens, os outros dois nós ainda conseguem formar uma maioria e executar o algoritmo de _Raft_ corretamente.

    Por isso, foi necessário ajustar o teste para incluir dois nós maliciosos. Assim, garantimos que há sempre eleições a decorrer, impedindo a verificação da propriedade de _Log Liveness_.

    Ainda assim, mesmo com dois nós maliciosos, o Stateright encontrou uma possibilidade de ainda haver líder na implementação do _spam_.
    Se um dos nós maliciosos começar uma eleição e o nó 2 responder positivamente, o nó que começou a eleição passa a ser o líder.

    #demonstration_subtitle[Propriedades Não Verificadas]
    - #link(<log_liveness>)[Vivacidade do Log (Log Liveness)]

  ]
][
  Para este problema, não há uma solução concreta, e teremos que recorrer a soluções heurísticas.

  Uma solução possível é ignorar começos de eleições demasiado cedo, por exemplo, se um _term_ começou há menos de 1 minuto, não vamos participar em eleições futuras. No entanto, esta solução pode fazer com que o sistema demore demasiado tempo a voltar ao normal caso o líder realmente morra nesse intervalo de tempo.

  Outra solução possível é limitar o número de eleições que um nó pode iniciar num dado espaço de tempo, por exemplo, 1 eleição por hora.
][
  Optámos por uma abordagem mais simples: se um nó iniciar eleições em três ocasiões consecutivas, esse nó é colocado na lista negra (_black-listed_). Desta forma, previne-se que nós maliciosos possam continuamente desestabilizar o sistema através do _spam_ de eleições.

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
      - O nó malicioso é #text(fill: red)[black-listed];
        - Expectativa temporal: `Sometimes`

    Todas as propriedades de segurança foram verificadas com sucesso. No entanto, apenas demonstraremos a sequência de eventos que levaram à verificação do nó malicioso estar na lista negra (_black-listed_).

    #demonstration_subtitle[Sequência de Eventos]
    + `Timeout(Id(0), ElectionTimeout)`
    + `Id(0)` $->$ `Raft(VoteRequest(VoteRequestMessage { node_id: Id(0), current_term: 1, log_length: 0, last_log_term: 0 }))` $->$ `Id(2)`
    + `Id(2)` $->$ `Raft(VoteResponse(VoteResponseMessage { node_id: Id(2), current_term: 1, vote_granted: true }))` $->$ `Id(0)`
    + `Id(0)` $->$ `Raft(VoteRequest(VoteRequestMessage { node_id: Id(0), current_term: 2, log_length: 0, last_log_term: 0 }))` $->$ `Id(2)`
    + `Id(2)` $->$ `Raft(VoteResponse(VoteResponseMessage { node_id: Id(2), current_term: 2, vote_granted: true }))` $->$ `Id(0)`
    + `Id(0)` $->$ `Raft(VoteRequest(VoteRequestMessage { node_id: Id(0), current_term: 3, log_length: 0, last_log_term: 0 }))` $->$ `Id(2)`

    Neste cenário, o nó malicioso (0) inicia três eleições consecutivas, sendo que o nó 2 recebe todos esses pedidos. Após a terceira tentativa, o nó 0 é colocado na lista negra (_black-listed_) do nó 2, impedindo-o de continuar a desestabilizar o sistema.

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
    - Nó Malicioso é #text(fill: red)[black-listed];
    - #link(<election_safety>)[Segurança da Eleição (Election Safety)]
    - #link(<election_liveness>)[Vivacidade de Eleições (Election Liveness)]
    - #link(<log_liveness>)[Vivacidade do Log (Log Liveness)]
  ]
]

#pagebreak()

#fault_chapter[
  Bifurcação de _Log_
][
  Um nó malicioso pode enviar, no mesmo índice e termo, duas versões diferentes do _log_ para _subsets_ distintos de nós. Com isto, diferentes _quorum's_ de nós recebem versões diferentes do _log_, o que faz proporciona uma bifurcação do _log_.
][
  Se dois conjuntos de nós aplicarem entradas diferentes no mesmo índice, a propriedade de _*Log Safety*_ é violada: leituras ou escritas podem comportar-se de forma inconsistente em caso de falhas do líder.
][
  Para demonstrar essa vulnerabilidade, podemos considerar um cenário em que um nó malicioso, a cada `LogRequest`, incrementa todos os valores das entradas do _log_ em uma unidade antes de enviá-las. Esse comportamento resulta em divergência entre as entradas _committed_ dos diferentes nós, comprometendo a consistência do sistema.

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

    Observa-se que, no evento n.º0, o nó 1 é eleito como líder. Já no evento n.º7, o nó 0 — que é malicioso — recebe a entrada do log, incrementa o valor e, assim, introduz uma inconsistência no log. Posteriormente, no próximo evento, ao receber a confirmação do nó 0, o nó 1 realiza o _commit_ dessa entrada adulterada, consolidando a inconsistência no sistema.

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
  Uma possível solução para esta falha, seria aquando a receção de um `LogRequest` contendo novas entradas do _log_, enviar para todos os outros nós, uma mensagem adicional que contivesse um _hash_ calculado sobre o _log_ atual (até à posição de _commit_). Desta forma, ao receber a mensagem, os outros nós fariam _hash_ do seu _log_ até à posição de _commit_ que receberam (ou até ao seu próprio índice de _commit_ caso seja maior) e comparariam os dois _hashes_. Caso fossem diferentes, o líder seria bloqueado pelos vários nós (_black_listed_) que detetassem a divergência, e começariam uma nova eleição.

  No entanto, para além desta solução ser custosa em termos de tráfego de mensagens, não haveria forma de reestabelecer o _log_, já que não saberemos qual seria o _log_ real.

  Para além disso, faria com que existisse outro problema, um nó malicioso poderia agora simplesmente criar _hashes_ falsos, podendo bloquear qualquer líder.

  Desta forma, decidimos não implementar uma solução para este problema.
][]

#pagebreak()

#fault_chapter[
  _Commit_ de _Log_ sem ser Aceite pela Maioria
][
  Um nó líder malicioso pode incrementar o `commit_index` sem ter recebido confirmação da maioria dos nós.
][
  Os _logs_ podem não estar atualizados numa maioria de nós, o que faria com que, caso o líder falhasse, dados fossem perdidos. Isto quebra o princípio de _safety_, pois o sistema pode considerar como _committed_ entradas que, na realidade, não foram replicadas na maioria dos nós.

  Por exemplo, imaginando que um líder recebe um pedido de escrita e adiciona a entrada ao seu _log_, mas, devido a uma falha de comunicação, apenas um dos seguidores recebe essa entrada, enquanto o outro não recebe nada. Se o líder for malicioso e incrementar o `commit_index` mesmo sem ter confirmação da maioria, ele pode responder ao cliente como se a operação estivesse garantida. Se este líder falhar imediatamente a seguir, e um novo líder for eleito entre os nós que não têm a entrada no _log_, a operação será perdida para sempre, apesar de já ter sido considerada _committed_ pelo sistema.

][
  Com o Stateright, podemos criar um nó malicioso que incrementa o `commit_index` para o tamanho do _log_ depois de qualquer mensagem ter sido enviada.

  #demonstration_box[
    *Localização:* src/fault/fake_commit_log.rs

    - *Atores:*
      - Id(0) --- #text(fill: red)[*Malicioso*]
      - Id(1) --- #text(fill: green.darken(50%))[Seguro]
      - Id(2) --- #text(fill: green.darken(50%))[Seguro]
      - Id(3) --- #text(fill: green.darken(50%))[Seguro]
      - Id(4) --- #text(fill: green.darken(50%))[Seguro]
    - Número de _crashes_ possíveis: 1
    - Mensagens possíveis:
      - `ClientRequest::Write { key: 1, value: 42, msg_id: 1 }`
      - `ClientRequest::Write { key: 2, value: 43, msg_id: 2 }`

    Apesar de termos configurado um ambiente de teste com 5 nós, permitindo mortes (_crashes_) e utilizando duas mensagens distintas para criar divergência nas entradas do _log_, o Stateright não detetou qualquer violação das propriedades. Não é claro se isto se deve a limitações na profundidade de exploração do Stateright, à inexistência efetiva do problema neste cenário, ou à impossibilidade de simular partições de rede entre os atores, uma vez que o Stateright não suporta este tipo de falha. Teríamos interesse em investigar mais a fundo as causas desta ausência de falhas detetadas, mas, devido a limitações de tempo, não nos foi possível fazê-lo.
  ]
][
  Este problema poderia ser resolvido, enviando uma assinatura digital como forma de autenticidade em cada `LogResponse`. Assim, o líder era obrigado a guardar essas assinaturas, para as propagar a seguir. Os outros nós só aceitariam `LogRequest`'s que incrementem o `commit_index`, se estes apresentassem assinaturas digitais válidas de um _quorum_ de nós.
][
  Da mesma forma que na solução da @falsificacao[], a natureza do problema não está relacionada com os conteúdos abordados na unidade curricular, portanto não iremos implementar a mitigação.
]

= Conclusão

Este trabalho prático permitiu ao grupo aprofundar significativamente o conhecimento sobre o algoritmo de _Raft_, as suas complexidades e as implicações de faltas assertivas na sua operação. A exploração de diferentes cenários de falhas e invetigação de mitigações eficazes proporcionaram uma perspetiva valiosa sobre o delicado balanço entre garantir a correção do sistema e manter um bom desempenho.

A utilização do Stateright revelou-se uma excelente escolha, visto oferecer uma experiência de utilização consideravelmente superior à do Maelstrom para a verificação formal de propriedades, identificação de vulnerabilidades e exploração de possíveis estados.

Adicionalmente, o grupo gostaria de ter tido a oportunidade de expandir a funcionalidade da aplicação principal. Seria interessante que, para além da integração com o Maelstrom, fosse possível executar nós _Raft_ com as faltas implementadas ativadas diretamente, dado que a "API de Faltas" foi concebida de forma genérica sobre a implementação do Raft. Outra área de exploração futura seria a integração mais dinâmica com o Stateright, permitindo, por exemplo, a execução da sua interface gráfica (UI) com atores e cenários definidos dinamicamente, facilitando uma análise ainda mais interativa e detalhada do comportamento do sistema.

#figure(
  block(
    inset: 1pt,
    radius: 5pt,
    stroke: 2pt + black,
    image("stateright_explorer.png", width: 100%),
  ),
  caption: [Interface gráfica do Stateright],
)
