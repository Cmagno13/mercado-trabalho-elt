# Mercado de trabalho por UF — ELT com pandas, dbt, Airflow, Superset e Amundsen

Pipeline ELT que baixa indicadores da PNAD Contínua (IBGE) para as 27 unidades da
federação, modela os dados com dbt e publica um dashboard no Superset. Tudo sobe com
um `docker compose up`.

![Dashboard no Superset](docs/dashboard.jpg)

## O que o projeto faz

```mermaid
flowchart LR
    A[API SIDRA / IBGE] -->|pandas| B[(raw.sidra_observacoes)]
    B -->|dbt| C[staging.stg_sidra__observacoes]
    S[seed uf_regiao] -->|dbt| D
    C -->|dbt| D[marts.dim_uf]
    C -->|dbt| E[marts.fct_mercado_trabalho_uf_trimestre]
    D --> E
    E --> F[Dashboard no Superset]
    G[Airflow: mercado_trabalho_elt] -.orquestra.-> B
    G -.orquestra.-> C
```

| Etapa | Ferramenta | Onde está |
| --- | --- | --- |
| Extração e carga | pandas + SQLAlchemy | `ingestion/sidra.py` |
| Transformação e testes de dados | dbt (Postgres) | `dbt/` |
| Orquestração | Apache Airflow 3 | `dags/mercado_trabalho_elt.py` |
| Visualização | Apache Superset 6 | `superset/bootstrap_dashboard.py` |
| Catálogo de dados (opcional) | Amundsen | `docker-compose.amundsen.yml`, `amundsen/load_catalog.py` |

**Dados:** duas séries trimestrais do SIDRA, de 2012 em diante, por UF:

- tabela 4099 — taxa de desocupação (%), pessoas de 14 anos ou mais;
- tabela 5436 — rendimento médio mensal real habitual de todos os trabalhos (R$).

O mart `fct_mercado_trabalho_uf_trimestre` tem uma linha por UF e trimestre, com os dois
indicadores e a variação de cada um contra o mesmo trimestre do ano anterior.

## Como rodar

Requisitos: Docker com Compose e cerca de 4 GB de disco para as imagens.

```bash
cp .env.example .env
docker compose up -d --build
```

Na primeira subida o Airflow executa o DAG sozinho (ingestão, `dbt seed`, `dbt run`,
`dbt test`, `dbt docs generate`) e, quando o mart existe, o serviço `superset-bootstrap` cria a conexão, o
dataset, os oito gráficos e o dashboard pela API do Superset.

| Serviço | Endereço | Acesso |
| --- | --- | --- |
| Airflow | http://localhost:8081 | sem login (modo demonstração) |
| Superset | http://localhost:8089/superset/dashboard/mercado-trabalho-uf/ | `admin` / `admin` |
| Postgres (warehouse) | `localhost:5440`, banco `warehouse` | `warehouse` / `warehouse` |

As credenciais são de demonstração e vêm do `.env`. Não exponha essa configuração fora
da sua máquina.

Para parar a pilha, use o comando `down` do Compose (com a opção de volumes, se quiser
apagar também os dados).

## Catálogo de dados com Amundsen (opcional)

O Amundsen sobe por um arquivo de Compose separado, porque acrescenta cinco serviços
(Neo4j, Elasticsearch e os três serviços do Amundsen) e cerca de 2 GB de memória.

```bash
docker compose -f docker-compose.yml -f docker-compose.amundsen.yml up -d
# depois que o DAG terminar:
docker compose -f docker-compose.yml -f docker-compose.amundsen.yml run --rm amundsen-loader
```

O carregador lê o `manifest.json` e o `catalog.json` que a tarefa `dbt_docs` do DAG grava
em um volume compartilhado e publica no Amundsen as tabelas, as colunas, as descrições
escritas no dbt e a linhagem entre os modelos. A busca fica em http://localhost:5005.

![Tabela do mart no Amundsen](docs/amundsen.jpg)

Notas de compatibilidade:

- As imagens oficiais do Amundsen só existem para amd64 e a mais recente é de março de
  2024. Em Apple Silicon elas rodam emuladas.
- O Neo4j usado é o 4.4 (imagem nativa para ARM) no lugar do 3.5 do Compose oficial do
  Amundsen, que não iniciou sob emulação. Por isso o serviço de metadados roda com
  `PROXY_ENCRYPTED=False`.
- O extrator de dbt do Amundsen converte os textos para minúsculas.

## Desenvolvimento sem o Airflow

```bash
uv sync
docker compose up -d postgres
export WAREHOUSE_URL=postgresql+psycopg2://warehouse:warehouse@localhost:5440/warehouse
uv run python -m ingestion.sidra
cd dbt && uv run dbt build --profiles-dir .
```

Testes e lint:

```bash
uv run pytest        # o teste de integração da carga só roda com WAREHOUSE_URL definido
uv run ruff check .
```

## Decisões de projeto

- **Carga completa, não incremental.** As séries são pequenas (2.970 linhas na carga de
  18/11/2026) e o IBGE revisa valores passados; substituir o conteúdo a cada execução é
  mais simples e evita divergência.
- **`TRUNCATE` + `INSERT` na tabela bruta.** Recriar a tabela falha na segunda execução,
  porque a view de staging do dbt depende dela. Há um teste de integração cobrindo isso.
- **dbt em um virtualenv separado dentro da imagem do Airflow.** As dependências do dbt
  conflitam com as do Airflow.
- **Dashboard como código.** Os gráficos são criados pela API REST do Superset, com
  tipos nativos, e o script pode ser executado mais de uma vez.
- **Visual do dashboard em CSS.** Uma cor por região em todos os gráficos, KPIs
  centralizados com fonte que acompanha a largura da tela e, abaixo de 900 px, os
  gráficos empilham e são ampliados para ocupar a largura.
- **Qualidade de dados no dbt.** Testes de chave única, integridade entre fato e
  dimensão, valores aceitos, taxa entre 0 e 100 e presença das 27 UFs.

## Limitações

- As médias por região no dashboard são **médias simples das UFs**, não a taxa oficial da
  região (que é ponderada pela população). Os títulos dos gráficos dizem isso.
- Um único Postgres guarda o warehouse e os metadados do Airflow e do Superset. Serve
  para demonstração, não para produção.
- O Airflow roda em modo `standalone`, sem autenticação.
- O layout foi conferido em 1500 px e 820 px de largura. Em celular o Superset usa a
  largura de página de desktop e o navegador apenas reduz a escala; não testei em um
  aparelho real.
- No Amundsen, só as colunas descritas no dbt aparecem com descrição, e a carga do
  catálogo é manual (não é uma tarefa do DAG).

## Licença

MIT.
