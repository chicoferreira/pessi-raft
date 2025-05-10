#set text(font: "IBM Plex Sans", lang: "pt", region: "pt")

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

#let fault_counter = counter("fault")

#let fault_subchapter_text_content(title, content) = grid(
  row-gutter: 0.6em,
  text(size: 1.1em, weight: "semibold", title),
  text(content),
)

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
  [#heading(numbering: "A.", supplement: [Falta], title) #label(title_label)],
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
  Como não há um líder estável, não há tempo suficiente para replicar as entradas no log, então o sistema não consegue fazer progresso.
][
  #text(fill: red)[A FAZER]
][
  Para este problema, não há uma solução concreta, e teremos que recorrer a soluções heurísticas.

  Uma solução possível é ignorar começos de eleições demasiado cedo, por exemplo, se um _term_ começou há menos de 1 minuto, ignorar o começo dele. No entanto, esta solução pode fazer com que o sistema volte demasiado tempo a voltar ao normal caso o líder realmente morra nesse espaço de tempo.

  Outra solução possível é limitar o número de eleições que um nodo pode iniciar num dado espaço de tempo, por exemplo, 1 eleição por hora.
][
  #text(fill: red)[A FAZER]
]

#fault_chapter[
  Enviar mensagens de _log_ sem ser o líder
][
  Um nodo malicioso pode enviar mensagens de _log_ (`AppendEntries`) sem ser o líder. Na implementação atual, sempre que um nodo
  recebe uma mensagem de _log_, verifica se o seu termo é maior ou igual ao termo da mensagem, e se for, declara imediatamente o nodo como líder.
][
  Qualquer nodo pode tornar-se líder, mesmo não tendo recebido votações positivas de outros nodos.
][
  #text(fill: red)[A FAZER]
][
  Uma possível solução para este problema, será a mesma do que a solução para o problema do voto duplo, que é adicionar uma mensagem adicional, por exemplo, `ElectedBy`, que é enviada num fim de eleição, por todos os nodos que foram eleitos para todos os outros nodos, com a lista de nodos que votaram nele. Caso os outros nodos detectarem que o líder malicioso não foi eleito por ele, podem começar uma nova eleição e bloquear o líder malicioso.

  Como um líder malicioso teria que enviar um quorum de nodos no `ElectedBy` para este ser válido, um quorum de nodos não votou nele, e portanto um quorum de nodos o bloquearia, impedindo que ele se torne líder futuramente.
][
  #text(fill: red)[A FAZER]
]

#fault_chapter[
  _Commit_ de _log_ sem ser aceite pela maioria
][
  Um nodo líder malicioso pode incrementar o `commit_index` sem ter recebido confirmação da maioria dos nodos.
][
  Os _logs_ podem não estar atualizados numa maioria dos nodos, o que faria com que caso o líder falhasse, dados seriam perdidos. Quebra o princípio de *?????*. #text(fill: red)[METER PRINCÍPIO].
][
  #text(fill: red)[A FAZER]
][
  É possível mitigar este problema recorrendo a assinaturas digitais, que permitiriam identificar a autenticidade dos votos de outros nodos.
][
  Da mesma forma que na solução da @falsificacao, devido à natureza do problema, que não está relacionada com os conteúdos abordados na unidade curricular, não iremos implementar a mitigação.
]
