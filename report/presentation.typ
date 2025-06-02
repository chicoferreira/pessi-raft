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
  authors: [Diogo Marques, Francisco Ferreira e Ivan Ribeiro],
)

#show link: underline
#show link: text.with(fill: blue)

#title-slide[
  = Implementação do Algoritmo Raft
]


#slide(title: [Implementação do Algoritmo Raft])[
  - Implementado em Rust.
  - Ao ser executado, inicializa um nó _Maelstrom_ que fica à escuta de mensagens do `stdin` e escreve para o `stdout`.

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

  No entanto, o teste não garante que o algoritmo esteja completamente correto.

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
  Demonstração pode ser feita alterando o campo em que é enviado o `node_id` das mensagens.

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

  #reddy[*Novo problema:* Como garantir que um nó malicioso não possa alterar a lista de votantes?]
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

#current_fault.update[Falta C - Negação de Serviço por Spam de Eleições]

#slide(title: context current_fault.get())[
  *Identificação da Falta*:\
  Nó malicioso pode ignorar _timer_ de eleições e enviar sempre `VoteRequest` para todos os outros nós, incrementando o termo de eleição.

  *Impacto:*\
  Não há líder estável, portanto propriedade de *Log Liveness* é violada.
]

#slide(title: context current_fault.get())[
  *Demonstração do Impacto*:\
  A cada mensagem recebida, o nó malicioso começa uma nova eleição.

  *Ambiente de Teste:*\
  - Atores: Id(0): #reddy[Malicioso], Id(1): #reddy[Malicioso], Id(2): #greeny[Legítimo]

  Dois atores devido a que, se fosse apenas um, e os nós 1 e 2 não recebessem as mensagens do nó 0 (_delay_ na rede), o sistema conseguiria progredir.

  *Propriedades Não Verificadas*:\
  - Vivacidade do Log (Log Liveness)
]

#slide(title: context current_fault.get())[
  *Medidas de Mitigação*:\
  Não há solução concreta. Apenas heurísticas.

  Possíveis soluções rondam a ideia de limitar o número de eleições consecutivas ou num certo período de tempo.

  Valores concretos terão que ser ajustados para obter um comportamento acertado.
]

#slide(title: context current_fault.get())[
  *Demonstração das Medidas de Mitigação*:

  Optámos por uma abordagem mais simples:
  - se um nó iniciar eleições em três termos consecutivos, é adicionado à _black-list_.

  *Ambiente de Teste:*
  - Atores: Id(0): #reddy[Malicioso], Id(1): #bluey[L. com mitigação], Id(2): #bluey[L. com mitigação]
  - Propriedades a verificar:
    + Nó malicioso é adicionado à _black-list_.
    + Segurança da Eleição (Election Safety)
    + Vivacidade do Log (Log Liveness)
    + Vivacidade de Eleições (Election Liveness)
    + Coerência do Log (Log Safety)
]

#slide(title: context current_fault.get())[
  *Sequência de Eventos #footnote[que levaram ao nó malicioso ser adicionado à _black-list_.]*:

  + `Timeout(Id(0), ElectionTimeout)`
  + `Id(0)` $->$ `VoteRequest { node_id: Id(0), current_term: 1, log_length: 0, last_log_term: 0 }` $->$ `Id(2)`
  + `Id(2)` $->$ `VoteResponse { node_id: Id(2), current_term: 1, vote_granted: true }` $->$ `Id(0)`
  + `Id(0)` $->$ `VoteRequest { node_id: Id(0), current_term: 2, log_length: 0, last_log_term: 0 }` $->$ `Id(2)`
  + `Id(2)` $->$ `VoteResponse { node_id: Id(2), current_term: 2, vote_granted: true }` $->$ `Id(0)`
  + `Id(0)` $->$ `VoteRequest { node_id: Id(0), current_term: 3, log_length: 0, last_log_term: 0 }` $->$ `Id(2)`
]

#slide(title: context current_fault.get())[
  *Estado Válido Encontrado#footnote[Neste estado, só o nó 2 recebeu os pedidos do nó malicioso.]*:
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

  *Propriedades Verificadas*: #set text(size: 0.9em)
  - Nó malicioso é #reddy[adicionado à _black-list_] ;
  - Segurança da Eleição (Election Safety);
  - Vivacidade de Eleições (Election Liveness);
  - Vivacidade do Log (Log Liveness);
  - Coerência do Log (Log Safety);
]

#current_fault.update[Falta D - Bifurcação de _Log_]

#slide(title: context current_fault.get())[
  *Identificação da Falta*:\
  Um nó malicioso pode enviar, no mesmo índice e termo, duas versões diferentes da entrada do log para nós diferentes.

  *Impacto:*\
  Se dois conjuntos de nós aplicarem entradas diferentes no mesmo índice e termo, a propriedade de *Coerência do Log* é violada.
]

#slide(title: context current_fault.get())[
  *Demonstração do Impacto*:\
  Podemos injetar no código de ao receber um `LogRequest`, incrementar todos os valores das entradas do log do nó malicioso.
  Fazendo com que se simule que este nó tenha recebido um nó diferente dos outros.
  #footnote[Podia haver uma demonstração mais clara. Limitado pela "API de Faltas", que não permite injeção de código no envio de mensagens. Futura melhoria.]

  *Ambiente de Teste:*
  - Atores: Id(0): #reddy[Malicioso], Id(1): #greeny[Legítimo], Id(2): #greeny[Legítimo]
]

#slide(title: context current_fault.get())[
  *Sequência de Eventos*:
  #set text(size: 0.8em)
  + `Timeout(Id(1), ElectionTimeout)`
  + `Id(1)` $->$ `VoteRequest { node_id: Id(1), current_term: 1, log_length: 0, last_log_term: 0 }` $->$ `Id(0)`
  + `Id(0)` $->$ `VoteResponse { node_id: Id(0), current_term: 1, vote_granted: true }` $->$ `Id(1)`
  + `Id(1)` $->$ `LogRequest { leader_id: Id(1), current_term: 1, prefix_length: 0, prefix_term: 0, commit_length: 0, suffix: [] }` $->$ `Id(0)`
  + `Id(0)` $->$ `LogResponse { node_id: Id(0), current_term: 1, ack: 0, success: true }` $->$ `Id(1)`
  + `Id(1)` $->$ `ClientRequest(Write { key: 1, value: 42, msg_id: 1 })` $->$ `Id(1)`
  + `Id(1)` $->$ `LogRequest { leader_id: Id(1), current_term: 1, prefix_length: 0, prefix_term: 0, commit_length: 0, suffix: [LogEntry { term: 1, from: Id(1), request: Write { key: 1, value: 42, msg_id: 1 } }] }` $->$ `Id(0)`
  + `Id(0)` $->$ `LogResponse { node_id: Id(0), current_term: 1, ack: 1, success: true }` $->$ `Id(1)`
]

#slide(title: context current_fault.get())[
  *Medidas de Mitigação*:\
  Ao receber um `LogRequest`, enviar para todos os outros nós o _hash_ do _log_ atual até ao `commit_length`. Caso divergir, declarar o líder como _black-list_ e começar nova eleição.

  #reddy[*No entanto há problemas acrescidos:*]
  - Custoso em termos de mensagens;
  - Não haveria forma de reestabelecer o _log_ original;
  - Nó malicioso podia fabricar _hashes_ erradas de modo a banir nós legítimos.

  Decidimos não implementar uma solução para este problema.
]

#current_fault.update[Falta E - _Commit_ de _Log_ sem ser aceite pela maioria]

#slide(title: context current_fault.get())[
  *Identificação da Falta*:\
  Um nó líder malicioso pode incrementar o `commit_length` sem ter recebido confirmação da maioria dos nós.

  *Impacto:*\
  - Os _logs_ podem não estar replicados numa maioria de nós, o que pode levar a inconsistências no sistema.
  - Quebra princípio de *Log Safety*, pois o sistema considera _logs_ como _committed_ mesmo que não tenham sido aceites pela maioria dos nós.
]

#slide(title: context current_fault.get())[
  *Demonstração do Impacto*:\
  Podemos criar um nó malicioso que, quando é líder, incrementa o `commit_length` para o tamanho do _log_ depois de qualquer mensagem ter sido enviada.

  *Ambiente de Teste:*
  - Atores: Id(0): #reddy[Malicioso], Id(1): #greeny[Legítimo], Id(2): #greeny[Legítimo], Id(3): #greeny[Legítimo], Id(4): #greeny[Legítimo]
  - Número de _crashes_ possíveis: 5
  - Mensagens possíveis:
    - `ClientRequest::Write { key: 1, value: 42, msg_id: 1 }`
    - `ClientRequest::Write { key: 2, value: 43, msg_id: 2 }`
]

#slide(title: context current_fault.get())[
  Apesar de aumentarmos as variáveis possíveis no ambiente de teste, o _Stateright_ não encontrou qualquer violação das propriedades.

  *Porquê?*

  - Limitações da exploração do espaço de estados?

  - Impossibilidade de simular partições de rede?

  - Inexistência do problema?


  Não conseguimos encontrar o motivo a tempo útil.
]

#slide(title: context current_fault.get())[
  *Medidas de Mitigação*:\

  Obrigar ao líder a acumular assinaturas digitais que são produzidas quando os nós enviam pacotes de resposta `LogResponse` para o líder.

  Os outros nós só aceitariam pacotes `LogRequest` que incrementam o `commit_length` se apresentarem uma assinatura digital válida de uma maioria dos nós.

  *Demonstração das Medidas de Mitigação:*\
  Não relacionado com a unidade curricular.
]

#title-slide[Conclusão]

#slide(title: [Conclusão])[
  *O que foi positivo:*
  - Excelente escolha do _Stateright_.
  - Aprofundamento do conhecimento sobre o algoritmo de _Raft_.

  *O que melhorar:*
  - Executar nós _Raft_ com as faltas implementadas ativadas dinamicamente.
  - Integração mais direta com o _Stateright_:
    + Permitindo a execução da sua UI, por exemplo.
    + Cenários de teste definidos dinamicamente;
]

#slide(title: [Conclusão])[
  #figure(image("stateright_explorer.png"), caption: [Interface gráfica do _Stateright_])
]

