#import "@preview/typslides:1.2.6": *

#show: typslides.with(
  ratio: "16-9",
  theme: "bluey",
)

#set text(font: "IBM Plex Sans", lang: "pt", region: "pt")
#set par(justify: true)

#set raw(lang: "rs")

#front-slide(
  title: [Tolerância a Faltas],
  subtitle: [Apresentação Trabalho Prático],
  authors: [Diogo Marques, Francisco Ferreira, Ivan Ribeiro],
)

#show link: underline
#show link: text.with(fill: blue)

#title-slide[
  = Implementação do Algoritmo Raft
]


#slide(title: [Implementação do Algoritmo Raft])[
  - Implementado em Rust.
  - Ao executar, inicializa um nó _Maelstrom_ que fica à escuta de mensagens do `stdin` e escreve para o `stdout`.

  #framed(title: [Teste com o _Maelstrom_])[
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
  ]

  #framed(title: [Resultado do teste], raw(lang: "rs", "> Everything looks good!"))

  No entanto, não garante que o algoritmo esteja completamente correto.

  O _Maelstrom_ só testa um subconjunto limitado de cenários e não garante a cobertura de todos os estados possíveis ou falhas do sistema.
]

#title-slide[
  = Verificação de Estados via Stateright
]

#slide(title: [Verificação de Estados via Stateright])[
  = O que é o Stateright?

  #v(2em)

  - Ferramenta de model checking para sistemas distribuídos escritos em Rust.
  - Permite explorar exaustivamente os possíveis estados de um sistema e verificar se certas propriedades se mantêm.
  - *Tudo a partir de código.*
]

#slide(title: [Funcionamento e Aplicação ao Raft])[
  O _model checking_ com Stateright envolve a definição de:

  - *Modelo:* Implementação do _Raft_, como modelo;
  - *Ambiente:* Rede que não perde, duplica ou reordena mensagens;
  - *Propriedades:*
    + *Segurança de Eleição*: No máximo um líder deve poder ser eleito num mesmo termo.
    + *Coerência do Log*: Se dois _logs_ contêm uma entrada _commited_ com o mesmo índice e termo, então as entradas são idênticas.
    + *Vivacidade do Log*: O _log_ não deverá ficar vazio eternamente.
    + *Vivacidade de Eleições*: Um líder deverá ser eleito inevitavelmente.
]

#slide(title: [Funcionamento e Aplicação ao Raft])[
  Propriedades encontram-se implementadas em cima de código do algoritmo _Raft_.
  #framed(
    title: [Exemplo de Propriedade],
    ```rs
    fn election_liveness_property(state: &ActorModelState) -> bool {
        state
            .actor_states
            .iter()
            .any(|s| s.current_role() == Role::Leader)
    }
    ```,
  )

  #framed(
    title: [Exemplo de Uso],
    ```rs
    ActorModel::new((), ())
      .actors(vec![RaftActor::new(...), RaftActor::new(...), ...])
      .property(Sometimes, election_liveness_property)
      .property(Sometimes, log_liveness_property)
      .property(Always, election_safety_property)
      .property(Always, log_safety_property)
      .max_crashes(1)
      .checker()
      .target_max_depth(15)
      ...
    ```,
  )
  // Assim, conseguimos executar o Raft diretamente sobre este ambiente, facilitando a verificação formal das respetivas propriedades.
]

#title-slide[Demonstração e Mitigações de Faltas]

#slide(title: [Faltas])[
  *API de Faltas:* Injeção de código em pontos críticos da implementação do _Raft_.
  // ex: receção de mensagens, transição de estado (líder eleito), etc.

  - Conseguimos simular anomalias sem alterar o código do algoritmo.
  - Avaliação de cada falha via testes unitários (`cargo test`).
  - Verificar se as propriedades são violadas.
]

#let current_fault = state("current_fault", "")

#current_fault.update[Falta A - Falsificação/Adulteração de Mensagens]

#slide(title: context current_fault.get())[
  *Identificação da Falta*:\
  Um nó malicioso pode falsificar ou adulterar mensagens de outros nós.

  *Impacto:*\
  Extremamente grave. Comprometerá completamente a segurança do _cluster_.
  Na prática:
  - Derrubar eleições legítimas;
  - Eleger-se a si próprio, ao manipular votos de outros nós;
  - Causar divisões no _cluster_, enviando mensagens contraditórias para diferentes nós;
  - Impedir propagação de comandos legítimos.
]

#slide(title: context current_fault.get())[
  *Demonstração do Impacto*:\
  Demonstração pode ser feita alterando os campos onde é enviado o `node_id` da mensagem.

  ```rs
  struct VoteRequestMessage {
    node_id: Id,
    current_term: TermId,
    log_length: usize,
    last_log_term: TermId,
  }
  ```

  No _Stateright_, encontramos um caso que viola a propriedade de *Election Safety*.
]

#slide(title: context current_fault.get())[
  *Ambiente de Teste*:
  - Atores: Id(0): #reddy[Malicioso], Id(1): #greeny[Legítimo], Id(2): #greeny[Legítimo]

  *Sequência de Eventos*:
  #set text(size: 0.9em)
  + `Timeout(Id(1), ElectionTimeout)`
  + `Id(1)` $->$ `VoteRequest { node_id: Id(1), current_term: 1, log_length: 0, last_log_term: 0 }` $->$ `Id(2)`
  + `Id(2)` $->$ `VoteResponse { node_id: Id(2), current_term: 1, vote_granted: true }` $->$ `Id(1)`
  + `Timeout(Id(0), ElectionTimeout)`
  + `Id(0)` $->$ `VoteResponse { node_id: Id(1), current_term: 1, vote_granted: true }` $->$ `Id(0)`
]

#slide(title: context current_fault.get())[
  *Estado Inválido Encontrado*:
  #grid(
    columns: 3,
    column-gutter: 1fr,
    ```rs
    Node {
      id: Id(0),
      current_role: Leader,
      current_term: 1,
      ...
    }
    ```,
    ```rs
    Node {
      id: Id(1),
      current_role: Leader,
      current_term: 1,
      ...
    }
    ```,
    ```rs
    Node {
      id: Id(2),
      current_role: Follower,
      current_term: 1,
      ...
    }
    ```,
  )

  *Propriedades Violadas*:
  - Segurança da Eleição (Election Safety)
]

#slide(title: context current_fault.get())[
  *Medidas de Mitigação*:\
  Assinaturas digitais.

  *Demonstração das Medidas de Mitigação*:\
  Não relacionado com a unidade curricular.
]

#current_fault.update[Falta B - Voto Duplo]

#slide(title: context current_fault.get())[
  *Identificação da Falta*:\
  Um nó malicioso pode votar em dois candidatos diferentes numa eleição no mesmo termo.

  *Impacto:*\
  Permite que dois nós sejam eleitos como líder, o que quebra a propriedade de *Election Safety*.
]

#slide(title: context current_fault.get())[
  *Demonstração do Impacto*:\
  Podemos injetar código na receção do `VoteRequest` e responder sempre positivamente ao voto, sem verificar se o nó já votou noutro candidato.

  Enviar `VoteResponse` com `vote_granted: true` para qualquer `VoteRequest`.
]

#slide(title: context current_fault.get())[
  *Ambiente de Teste*:
  - Atores: Id(0): #reddy[Malicioso], Id(1): #greeny[Legítimo], Id(2): #greeny[Legítimo]

  *Sequência de Eventos*:
  #set text(size: 0.8em)
  + `Timeout(Id(0), ElectionTimeout)`
  + `Timeout(Id(2), ElectionTimeout)`
  + `Id(0)` $->$ `VoteRequest { node_id: Id(0), current_term: 1, log_length: 0, last_log_term: 0 }` $->$ `Id(1)`
  + `Id(0)` $->$ `VoteRequest { node_id: Id(0), current_term: 1, log_length: 0, last_log_term: 0 }` $->$ `Id(2)`
  + `Id(2)` $->$ `VoteRequest { node_id: Id(2), current_term: 1, log_length: 0, last_log_term: 0 }` $->$ `Id(0)`
  + `Id(0)` $->$ `VoteResponse { node_id: Id(0), current_term: 1, vote_granted: true }` $->$ `Id(2)`
  + `Id(1)` $->$ `VoteResponse { node_id: Id(1), current_term: 1, vote_granted: true }` $->$ `Id(0)`
]

#slide(title: context current_fault.get())[
  *Estado Inválido Encontrado*:
  #grid(
    columns: 3,
    column-gutter: 1fr,
    ```rs
    Node {
      id: Id(0),
      current_role: Leader,
      current_term: 1,
      ...
    }
    ```,
    ```rs
    Node {
      id: Id(1),
      current_role: Follower,
      current_term: 1,
      ...
    }
    ```,
    ```rs
    Node {
      id: Id(2),
      current_role: Leader,
      current_term: 1,
      ...
    }
    ```,
  )

  *Propriedades Violadas*:
  - Segurança da Eleição (Election Safety)
]

#slide(title: context current_fault.get())[
  *Medidas de Mitigação*:\
  Mensagem adicional `ElectedBy` difundida no fim da eleição por todos os nós eleitos no fim da eleição, com a lista de nós que votaram neles.

  Se um nó votou em mais de um candidato, este pode ser adicionado a uma _black-list_.

  #reddy[*Novo problema:* como garantir que um nó malicioso não possa alterar a lista de votantes?]
]

#slide(title: context current_fault.get())[
  *Demonstração das Medidas de Mitigação*:\

  #v(0.5em)

  *Ambiente de Teste:* \
  - Atores: Id(0): #reddy[Malicioso], Id(1): #bluey[L. com mitigação], Id(2): #bluey[L. com mitigação]

  *Propriedades a verificar:*
  - O nó malicioso é adicionado à _black-list_.

  O nó malicioso ainda pode fazer com que outro nó se torne líder imediatamente após a eleição (Violação da propriedade de *Election Safety*).

  Não há problema porque é invocada uma nova eleição de seguida.
  Mas não podemos verificar todas as propriedades.
]

#slide(title: context current_fault.get())[
  *Sequência de Eventos*:
  #set text(size: 0.81em)
  + `Timeout(Id(2), ElectionTimeout)`
  + `Id(2)` $->$ `VoteRequest { node_id: Id(2), current_term: 1, log_length: 0, last_log_term: 0 }` $->$ `Id(0)`
  + `Timeout(Id(1), ElectionTimeout)`
  + `Id(1)` $->$ `VoteRequest { node_id: Id(1), current_term: 1, log_length: 0, last_log_term: 0 }` $->$ `Id(0)`
  + `Id(2)` $->$ `VoteRequest { node_id: Id(2), current_term: 1, log_length: 0, last_log_term: 0 }` $->$ `Id(1)`
  + `Id(0)` $->$ `VoteResponse { node_id: Id(0), current_term: 1, vote_granted: true }` $->$ `Id(1)`
  + `Id(0)` $->$ `VoteResponse { node_id: Id(0), current_term: 1, vote_granted: true }` $->$ `Id(2)`
  + `Id(1)` $->$ `Other(ElectedBy { leader: Id(1), term: 1, by: [Id(1), Id(0)] })` $->$ `Id(1)`
  + `Id(2)` $->$ `Other(ElectedBy { leader: Id(2), term: 1, by: [Id(2), Id(0)] })` $->$ `Id(1)`
]

#slide(title: context current_fault.get())[
  *Estado Inválido Encontrado*:
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

  (`ElectedBy` ainda não foi propagado para o nó 2)

  *Propriedades Verificadas*:
  - Nó malicioso (0) é adicionado à _black-list_.
]
