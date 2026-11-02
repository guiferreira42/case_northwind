# Northwind Logistics Lakehouse

Projeto de estudo e portfólio em Engenharia de Dados com Databricks, PySpark, SQL e Delta Lake. O objetivo é praticar um fluxo próximo ao trabalho de um ambiente produtivo: entender a demanda, organizar os dados, testar, revisar mudanças e operar o pipeline.

**Status:** estruturação inicial. As funcionalidades serão desenvolvidas em pequenas entregas, com commits, Pull Requests e evidências de execução.

## Objetivo

Disponibilizar tabelas confiáveis para analisar a expedição de pedidos da Northwind: pedidos sem envio, pendências vencidas, tempo até a expedição e cumprimento de prazo por transportadora e destino.

A entrega de engenharia termina nas tabelas Gold e na operação do pipeline. O [PRD](docs/product/PRD.md) define os requisitos e os critérios de aceite. O documento Word é a referência principal; o Markdown mantém o mesmo conteúdo para consulta e revisão no Git.

## Arquitetura

O teste completo acompanha o dado desde a origem:

**SQL Server on-premises → extração → Volume do Databricks → Bronze → Silver → Gold**

O pipeline no Databricks começa nos arquivos do Volume. Lakeflow Jobs coordenará as etapas, suas dependências e o registro dos resultados.

- **Bronze:** receber os dados em Delta, identificando arquivo, lote e momento da ingestão.
- **Silver:** padronizar tipos, tratar duplicatas, validar dados e preservar o histórico de clientes com SCD Tipo 2.
- **Gold:** organizar fatos, dimensões e indicadores prontos para consulta.

## Regras do projeto

- Os arquivos de entrada ficam nos Volumes e as tabelas processadas ficam no Databricks. Dados brutos e credenciais não entram no Git.
- Novos registros e alterações de teste devem ser aplicados no **SQL Server de origem on-premises** e passar novamente pela extração e por todas as camadas. Não inserir diretamente na Silver ou Gold para comprovar esse fluxo.
- Cada lote deve ser identificado e estar completo antes de ser processado. Repetir o mesmo lote não pode duplicar pedidos ou versões de cliente.
- Registros inválidos devem ter o motivo registrado. Erros críticos impedem declarar a carga concluída; falhas precisam de procedimento de recuperação.
- Os indicadores devem conferir com a entrada, sem multiplicar o frete pelo número de itens. Dados históricos usam uma data de referência explícita.
- As mudanças serão feitas em branches, com testes e revisão por Pull Request. Testes, agendamento e automações serão acrescentados conforme a implementação avançar.

## Organização inicial prevista

```text
case_northwind/
├── dataset/
│   └── scripts_insert/
├── docs/
├── models/
├── notebooks/
└── tests/
```

`dataset/scripts_insert/` reúne os scripts de teste da origem; `docs/` guarda os requisitos e a documentação técnica; `models/` organiza os modelos analíticos; `notebooks/` contém as entradas de execução; `tests/` reúne as verificações do pipeline. A estrutura será preenchida durante o desenvolvimento.

## Prática de engenharia

O ambiente utiliza a Databricks Free Edition, respeitando seus recursos e quotas. A proposta é aplicar práticas de produção: qualidade, rastreabilidade, reprocessamento seguro e recuperação, demonstradas com execuções verificáveis.

O avanço do portfólio será demonstrado pelo pipeline funcionando, pelos testes e pelas decisões registradas no histórico do projeto.
