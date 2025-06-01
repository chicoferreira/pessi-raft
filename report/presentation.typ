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

#slide(title: [Falta A - Falsificação/Adulteração de Mensagens])[
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

#slide(title: [Falta A - Falsificação/Adulteração de Mensagens])[
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

#slide(title: [Falta A - Falsificação/Adulteração de Mensagens])[
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

#slide(title: [Falta A - Falsificação/Adulteração de Mensagens])[
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

#slide(title: [Falta A - Falsificação/Adulteração de Mensagens])[
  *Medidas de Mitigação*:\
  Assinaturas digitais.

  *Demonstração das Medidas de Mitigação*:\
  Não relacionado com a unidade curricular.
]

