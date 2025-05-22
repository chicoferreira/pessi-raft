#set text(font: "IBM Plex Sans", lang: "pt", region: "pt")
#show link: underline
#show link: text.with(fill: blue)

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
      column-gutter: 1fr,
      name_and_number[Francisco Macedo Ferreira][PG55942],
      name_and_number[Ivan Sérgio Rocha Ribeiro][PG55950],
      name_and_number[Diogo Alexandre Correia Marques][PG?????],
    )
  ),
)

#v(2em)

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

#show "Stateright": link("https://github.com/stateright/stateright")[_Stateright_]

= Verificação de estados com o Stateright

Para além dos testes de integração com o Maelstrom, recorreu-se ao Stateright para uma verificação mais formal do algoritmo Raft implementado. O Stateright é uma ferramenta de _model checking_ para sistemas distribuídos escritos em Rust, que permite explorar exaustivamente os possíveis estados de um sistema e verificar se certas propriedades, a partir de código.


== Funcionamento e Aplicação ao Raft
O _model checking_ com Stateright envolve a definição de:
+ Um *modelo* do sistema: Este modelo descreve os estados possíveis de cada nodo Raft (e.g., `current_term`, `voted_for`, `log`, `commit_index`, `role`) e as ações que podem transitar o sistema de um estado para outro (e.g., enviar/receber uma mensagem `RequestVote`, `AppendEntries`, dar _timeout_ numa eleição).
+ Um *ambiente*: Define como as ações dos nodos interagem, incluindo a rede (que pode perder, duplicar ou reordenar mensagens) e outros eventos não determinísticos (mensagens que podem ser enviadas em qualquer momento)
+ *Propriedades*: São invariantes ou condições que devem ser sempre verdadeiras em todos os estados alcançáveis do sistema.

Esta definição foi feita através de:
- implementação do Raft, como modelo;
- ambiente com uma rede que não perde, duplica ou reordena mensagens;
- uma única mensagem, que consiste em escrever o valor `42` na chave `1`; #footnote[Esta mensagem foi escolhida de forma arbitrária e única para simplificar e acelerar a exploração dos possíveis estados do sistema, sendo suficiente para verificar todas as propriedades.]
- uma lista de propriedades que iremos apresentar a seguir.

== Propriedades Verificadas
Foram definidas e verificadas várias propriedades fundamentais do Raft para garantir a sua correção:

+ *Segurança da Eleição (Election Safety)*: No máximo um líder pode ser eleito num determinado termo.
  - Expectativa temporal: `Always`

+ *Coerência do Log (Log Safety)*: Se dois _logs_ contêm uma entrada _committed_ com o mesmo índice e termo, então os _logs_ são idênticos até esse índice.
  - Expectativa temporal: `Always`

+ *Vivacidade do Log (Log Liveness)*: O _log_ deverá progredir, isto é, haver um líder que aplique entradas no _log_. O _log_ não deverá ficar vazio.
  - Expectativa temporal: `Sometimes`

+ *Vivacidade de Eleições (Election Liveness)*: O sistema deverá progredir de modo a que um líder seja eleito. Um líder deverá ser eleito.
  - Expectativa temporal: `Sometimes`

Estas propriedades, que se encontram em `src/fault/property.rs`, foram verificadas com o Stateright para o algoritmo Raft implementado, com sucesso.

#pagebreak()

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

#fault_chapter(title_label: "falsificacao")[
  Falsificação/Adulteração de Mensagens
][
  Um nodo malicioso pode falsificar ou adulterar (via _man-in-the-middle_) mensagens de outros nodos.
][
  O impacto é total, um nodo malicioso pode tomar controlo total do cluster de nodos. Um nodo malicioso pode:
  - fazer com que eleições sejam derrubadas, falsificando mensagens de votação;
  - #text(fill: red)[A ADICIONAR MAIS]
][
  A demonstração de falsificação via Maelstrom é simples, visto que é possível colocar qualquer `"src"` na mensagem e o Maelstrom não valida a origem da mensagem. Fora do Maelstrom, num sistema com comunicação via IP, também é possível falsificar mensagens, por exemplo, com recurso a _IP Spoofing_.

  #text(fill: red)[A FAZER]
][
  Seria possível mitigar completamente este problema adicionando autenticação às mensagens, por exemplo, através de assinaturas digitais.
][
  Devido à natureza do problema, não relacionado com os conteúdos abordados na unidade curricular, não iremos implementar a mitigação.
]

#fault_chapter[
  Voto Duplo
][
  Um nodo malicioso pode votar em dois candidatos diferentes numa eleição.
][
  Permite que dois nodos sejam eleitos como líder, o que quebra a propriedade de *Election Safety* do algoritmo.
][
  #text(fill: red)[A FAZER]
][
  É possível resolver este problema com uma mensagem adicional, por exemplo, `ElectedBy`, que é enviada num fim de eleição, por todos os nodos que foram eleitos para todos os outros nodos, com a lista de nodos que votaram nele. Caso seja detetado um nodo que votou em dois candidatos diferentes, outros nodos podem ignorar o seu voto futuramente (_black-listed_).

  Daqui surge um novo problema, que é, um nodo bizantino pode dizer que recebeu votos de quem não votou nele, o que faria que esse outro nodo fosse _black-listed_ indevidamente. Uma solução possível a este problema é recorrer a assinaturas digitais, que permitiriam identificar a autenticidade do voto.
][
  #text(fill: red)[A FAZER (sem criptografia)]
]

#fault_chapter[
  Negação de Serviço por Spam de Eleições
][
  Cada _follower_ tem uma _timer_ para dar _timeout_ e começar uma eleição, quando não recebe atualizações de um líder. Um nodo malicioso pode simplesmente ignorar esse _timer_ e começar sempre uma nova eleição. A cada momento desses, o nodo malicioso, incrementa o seu _term_ e tenta eleger-se como líder. Como o seu _term_ é maior que o do líder, todos os nodos (incluindo o líder) votam nele, e ele passa a ser o líder.

  Este nodo malicioso pode continuar a fazer isto indefinidamente, sempre que um líder é eleito.
][
  Como não há um líder estável, a propriedade de _liveness_ não é respeitada, isto é, o sistema não consegue fazer progresso, visto que não há tempo suficiente para replicar as entradas no log.
][
  #text(fill: red)[A FAZER]

  Falar que o stateright encontrou uma possibilidade de ainda haver lider na implementação do spam.
][
  Para este problema, não há uma solução concreta, e teremos que recorrer a soluções heurísticas.

  Uma solução possível é ignorar começos de eleições demasiado cedo, por exemplo, se um _term_ começou há menos de 1 minuto, ignorar o começo dele. No entanto, esta solução pode fazer com que o sistema volte demasiado tempo a voltar ao normal caso o líder realmente morra nesse espaço de tempo.

  Outra solução possível é limitar o número de eleições que um nodo pode iniciar num dado espaço de tempo, por exemplo, 1 eleição por hora.
][
  #text(fill: red)[A FAZER]
]

#fault_chapter[
  Bifurcação de _log_
][
  Um nodo malicioso pode enviar, no mesmo índice e termo, duas versões diferentes do _log_ para _subsets_ distintos de nodos. Com isto, diferentes _quorum's_ de nodos recebem versões diferentes do _log_, o que faz com que o _log_ se bifurque.
][
  Se dois conjuntos de nodos aplicarem entradas diferentes no mesmo índice, a propriedade de _*Log Safety*_ é violada: leituras ou escritas podem comportar-se de forma inconsistente em caso de falhas do líder.
][
  #text(fill: red)[A FAZER]
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
  #text(fill: red)[A FAZER]
][
  Este problema poderia ser resolvido, enviando uma assinatura digital como forma de autenticidade e autenticação em cada `LogResponse`. Assim, o líder era obrigado a guardar essas assinaturas, para as propagar a seguir. Os outros nodos só aceitariam `LogRequest`'s que incrementem o `commit_index`, se nela conter assinaturas digitais válidas de um quorum de nodos.
][
  Da mesma forma que na solução da @falsificacao, devido à natureza do problema, que não está relacionada com os conteúdos abordados na unidade curricular, não iremos implementar a mitigação.
]
