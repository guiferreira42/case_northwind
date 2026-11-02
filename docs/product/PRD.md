# Dados de Expedição Northwind

Documento de Requisitos do Produto

Versão 2.1 | 02 de novembro de 2026 | Entrega de Engenharia de Dados

Demanda de referência para o portfólio. Responsáveis pela definição: PO de Logística e analista de negócios.

## Objetivo e contexto

Na reunião da manhã, a gestão precisa identificar pedidos sem envio e comparar os prazos de expedição. Hoje, responder exige juntar e conferir registros. A demanda é organizar esses dados no Databricks para que a mesma pergunta, com os mesmos filtros, produza o mesmo número.

Entregar tabelas Gold confiáveis, com origem conhecida e atualização controlada, para apoiar a priorização de pendências e a análise por transportadora e destino. O fluxo será Volume → Bronze → Silver → Gold, executado por Job.

## Público alvo e personas

| Persona | Necessidade |
| --- | --- |
| Gestor de logística | Identificar pendências vencidas e comparar o cumprimento de prazo. |
| Analista de negócio | Consultar indicadores e conferir os pedidos que explicam o total. |
| Engenheiro de dados | Executar cargas, investigar erros e recuperar o fluxo sem duplicar dados. |

### Resumo em 5W2H

| Pergunta | Resposta |
| --- | --- |
| O quê | Base Gold de expedição. |
| Por quê | Números confiáveis para decidir. |
| Quem | PO, analista e engenharia. |
| Onde | Databricks e GitHub. |
| Quando | Entregas por camada, testes e PR. |
| Como | PySpark, SQL, Delta e Jobs. |
| Quanto | Recursos da Free Edition, sem contratação paga prevista. |

<!-- pagebreak -->

## Requisitos funcionais

| ID | O que o produto deve fazer | Como conferir |
| --- | --- | --- |
| RF01 | Receber lotes completos do Volume. | Identificar arquivos, lote e contagens na Bronze Delta. |
| RF02 | Tratar e validar os dados na Silver. | Tipos corretos, chaves únicas e inválidos separados com motivo. |
| RF03 | Preservar versões do cliente. | Mudança de cidade/país cria nova versão sem apagar a anterior, com validade sem sobreposição. |
| RF04 | Publicar pedidos, itens, dimensões e indicadores na Gold. | Uma linha por pedido ou por pedido + produto, conforme a tabela; totais conferidos. |
| RF05 | Reprocessar sem duplicar resultados. | Repetir o lote mantém fatos e versões de cliente estáveis. |
| RF06 | Coordenar e recuperar a execução. | Job respeita dependências, registra resultados e permite retomar falhas. |

### Fluxo de uso

A entrada disponibiliza o lote completo. A engenharia executa o Job e confere as validações. Após aprovação, o consumidor consulta a Gold e os registros que explicam cada indicador. Uma falha impede declarar a entrega concluída.

### Regras de negócio e indicadores de expedição

| Indicador | Regra |
| --- | --- |
| Tempo até envio | Dias entre pedido e expedição, somente com datas válidas. |
| Percentual após prazo | Expedidos após a data requerida / expedidos com prazo conhecido. |
| Sem expedição | Pedidos com data de envio vazia. |
| Pendentes vencidos | Sem envio e com prazo anterior à data de referência. |
| Frete total | Somar uma vez por pedido, sem multiplicar pelos itens. |
| Receita líquida | Preço vendido do item × quantidade × (1 - desconto), sem frete; precisão e arredondamento documentados. |

Envio no dia requerido está no prazo. Sem envio não entra no percentual de expedidos. Prazo ausente é desconhecido; divisão sem pedidos elegíveis resulta em sem dados. Dois pedidos expedidos, um após prazo, representam 50%. Expedição não comprova entrega ao cliente.

<!-- pagebreak -->

## Requisitos não funcionais

| Critério | Requisito verificável |
| --- | --- |
| Desempenho | Registrar tempo por etapa e volume processado. Definir a janela-alvo após medir o lote de referência. |
| Segurança | Sem credenciais no Git ou nos logs. Acesso somente aos objetos necessários; consumidor lê a Gold autorizada. |
| Usabilidade | Nomes, significado de cada linha e fórmulas documentados; consultas de conferência e instruções claras de execução. |
| Confiabilidade | Rastrear lote, execução e versão do código. Repetição sem duplicação; erro crítico bloqueia a conclusão; recuperação testada. |
| Compatibilidade | Usar recursos disponíveis no workspace; registrar runtime, dependências e parâmetros para repetir a execução. |

## Premissas restrições e dependências

- Premissas: arquivos de pedidos, itens, clientes, produtos, categorias e transportadoras completos no Volume, com identificação e instante de referência. Conferir nomes, tipos e contagens antes de processar.

- Dependências: acesso ao Volume, tabelas e compute; fonte disponível para novos lotes; regra de arredondamento registrada no dicionário; validação das regras pelo PO e analista.

- Restrições: processamento no Databricks Free Edition, conforme quotas e recursos disponíveis. O escopo da implementação vai do Volume até a Gold; não há compromisso de SLA empresarial.

### Limites dos dados e do teste

Usar data de referência explícita nos dados históricos. O histórico de clientes começa no primeiro estado conhecido; não inventar versões anteriores. Fotografias de dados mostram mudanças entre lotes, não todos os eventos intermediários.

No teste ponta a ponta, criar ou alterar registros no SQL Server on-premises e percorrer nova extração → Volume → Bronze → Silver → Gold. Receber o lote inicial antes de alterar o cliente. A preparação da origem é anterior ao pipeline deste repositório.

<!-- pagebreak -->

## Métricas de sucesso

Acompanhar nas três primeiras cargas após a entrega e na validação de uso. As metas abaixo são propostas iniciais para confirmar com o PO; não representam resultados já medidos.

| Medida | Meta proposta | Como medir |
| --- | --- | --- |
| Consistência | Zero diferenças sem explicação. | Comparar contagens e valores da entrada com a Gold, usando as mesmas regras. |
| Qualidade publicada | 100% das publicações sem erro crítico. | Consultar validações e registros rejeitados de cada carga. |
| Repetição segura | 100% dos lotes repetidos sem duplicação lógica. | Comparar chaves, versões de cliente e totais antes/depois. |
| Utilidade para o negócio | Responder corretamente às três perguntas. | Gestor/analista confirma: quais estão sem envio, quais venceram e como as transportadoras se comparam. |

Tempo de processamento será acompanhado pelos logs do Job; a primeira medição servirá de referência para acordar a meta. Redução de atrasos ou de esforço manual exige medição de uso e não será presumida.

## Critérios de aceite

Definição de pronto para aprovar a entrega:

- [ ] Lote completo identificado e camadas persistidas em Delta.

- [ ] Contagens de recebidos, aceitos e rejeitados explicadas; totais da Gold conferidos.

- [ ] Histórico de cliente correto, sem sobreposição, e repetição sem versões extras.

- [ ] Indicadores conferidos com exemplos e registros de entrada, inclusive nulos e frete por pedido.

- [ ] Job completo executado; falha controlada recuperada sem duplicação.

- [ ] Requisitos não funcionais verificados e medição inicial de desempenho registrada.

- [ ] Código, testes, configuração e documentação revisados por PR, com evidências.

- [ ] PO/analista registra aceite das regras e respostas; engenharia registra a entrega operacional.

Na demonstração, ligar versão do código, lote, execução e resultado. Apresentar carga inicial, atualização na origem, repetição e recuperação. Arquitetura, dicionário e procedimento de execução detalham a implementação.
