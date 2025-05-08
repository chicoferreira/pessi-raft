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

#let fault_chapter(title, identification, impact, demonstration, mitigation, mitigation_demonstration) = grid(
  row-gutter: 1.1em,
  heading(numbering: "A.", supplement: [Falta], title),
  fault_subchapter_text_content([Identificação da Falta], identification),
  fault_subchapter_text_content([Impacto], impact),
  fault_subchapter_text_content([Demonstração do Impacto], demonstration),
  fault_subchapter_text_content([Medidas de Mitigação], mitigation),
  fault_subchapter_text_content([Demonstração das Medidas de Mitigação], mitigation_demonstration),
)

#fault_chapter[
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

