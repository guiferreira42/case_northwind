# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Análise exploratória da Northwind
# MAGIC
# MAGIC **Objetivo:** entender o conteúdo, a qualidade, o grão e as relações do modelo `northwind.bronze`, antes de propor transformações para Silver e Gold.
# MAGIC
# MAGIC Esta análise é somente leitura. Ela não cria nem altera tabelas. Os valores comerciais são estimativas de venda dos itens (`unit_price × quantity`), com e sem desconto; frete não é somado à receita e custo/margem não estão disponíveis no modelo de origem.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Escopo e inventário
# MAGIC
# MAGIC Começamos confirmando quais tabelas existem, seus tipos e volumes. Isso documenta o ponto de partida e ajuda a identificar tabelas ausentes antes de qualquer análise.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOG = "northwind"
SCHEMA = "bronze"
DATABASE = f"{CATALOG}.{SCHEMA}"

TABLES = [
    "categories",
    "customer_customer_demo",
    "customer_demographics",
    "customers",
    "employees",
    "employee_territories",
    "order_details",
    "orders",
    "products",
    "region",
    "shippers",
    "suppliers",
    "territories",
    "us_states",
]

available_tables = {
    row.tableName
    for row in spark.sql(f"SHOW TABLES IN {DATABASE}").collect()
}
missing_tables = sorted(set(TABLES) - available_tables)

if missing_tables:
    raise ValueError(
        f"Tabelas esperadas não encontradas em {DATABASE}: {missing_tables}. "
        "Confirme o catálogo e o schema antes de continuar."
    )

dfs = {name: spark.table(f"{DATABASE}.{name}") for name in TABLES}

inventory_rows = []
for name, df in dfs.items():
    inventory_rows.append(
        (
            name,
            df.count(),
            len(df.columns),
            ", ".join(f"{field.name}:{field.dataType.simpleString()}" for field in df.schema.fields),
        )
    )

inventory = spark.createDataFrame(
    inventory_rows,
    ["tabela", "linhas", "colunas", "tipos_de_dados"],
)
display(inventory.orderBy("tabela"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Perfil de completude
# MAGIC
# MAGIC Calculamos nulos em todas as colunas e strings vazias em colunas textuais. Percentuais tornam tabelas de tamanhos diferentes comparáveis. Nulo e string vazia são tratados separadamente porque representam problemas distintos na origem.

# COMMAND ----------

profile_rows = []
for table_name, df in dfs.items():
    row_count = df.count()
    expressions = []
    for column_name, field in zip(df.columns, df.schema.fields):
        safe_name = f"c{len(expressions)}"
        expressions.append(
            F.sum(F.when(F.col(column_name).isNull(), 1).otherwise(0)).alias(safe_name)
        )
        if isinstance(field.dataType, T.StringType):
            blank_name = f"c{len(expressions)}"
            expressions.append(
                F.sum(
                    F.when(
                        F.col(column_name).isNotNull()
                        & (F.trim(F.col(column_name)) == ""),
                        1,
                    ).otherwise(0)
                ).alias(blank_name)
            )
        else:
            blank_name = None

        profile_rows.append((table_name, column_name, safe_name, blank_name, row_count))

    if expressions:
        counts = df.agg(*expressions).first().asDict()
        for row_index in range(len(profile_rows) - len(df.columns), len(profile_rows)):
            table, column, null_key, blank_key, total = profile_rows[row_index]
            null_count = int(counts.get(null_key) or 0)
            blank_count = int(counts.get(blank_key) or 0) if blank_key else 0
            profile_rows[row_index] = (
                table,
                column,
                total,
                null_count,
                round(100 * null_count / total, 2) if total else None,
                blank_count,
                round(100 * blank_count / total, 2) if total else None,
            )

profile = spark.createDataFrame(
    profile_rows,
    [
        "tabela",
        "coluna",
        "linhas",
        "nulos",
        "pct_nulos",
        "strings_vazias",
        "pct_strings_vazias",
    ],
)
display(profile.orderBy(F.desc("pct_nulos"), F.desc("pct_strings_vazias"), "tabela", "coluna"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Grão e unicidade das chaves
# MAGIC
# MAGIC Validamos as chaves naturais esperadas no Northwind. `order_details` tem grão de item do pedido, portanto sua chave é composta por pedido e produto. Relações muitos-para-muitos também usam chaves compostas.

# COMMAND ----------

PRIMARY_KEYS = {
    "categories": ["category_id"],
    "customer_customer_demo": ["customer_id", "customer_type_id"],
    "customer_demographics": ["customer_type_id"],
    "customers": ["customer_id"],
    "employees": ["employee_id"],
    "employee_territories": ["employee_id", "territory_id"],
    "order_details": ["order_id", "product_id"],
    "orders": ["order_id"],
    "products": ["product_id"],
    "region": ["region_id"],
    "shippers": ["shipper_id"],
    "suppliers": ["supplier_id"],
    "territories": ["territory_id"],
    "us_states": ["state_id"],
}

key_quality_rows = []
for table_name, key_columns in PRIMARY_KEYS.items():
    df = dfs[table_name]
    null_predicate = None
    for key_column in key_columns:
        predicate = F.col(key_column).isNull()
        null_predicate = predicate if null_predicate is None else (null_predicate | predicate)

    null_key_rows = df.filter(null_predicate).count()
    duplicate_key_groups = (
        df.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    key_quality_rows.append(
        (table_name, ", ".join(key_columns), null_key_rows, duplicate_key_groups)
    )

key_quality = spark.createDataFrame(
    key_quality_rows,
    ["tabela", "chave_esperada", "linhas_com_chave_nula", "grupos_de_chave_duplicados"],
)
display(key_quality.orderBy(F.desc("linhas_com_chave_nula"), F.desc("grupos_de_chave_duplicados")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Integridade referencial
# MAGIC
# MAGIC Contamos chaves órfãs com anti-join. Esse diagnóstico ajuda a separar ausência legítima (por exemplo, pedido sem cliente informado) de referências que impediriam joins confiáveis nas camadas seguintes.

# COMMAND ----------

FOREIGN_KEYS = [
    ("orders", "customer_id", "customers", "customer_id"),
    ("orders", "employee_id", "employees", "employee_id"),
    ("orders", "ship_via", "shippers", "shipper_id"),
    ("order_details", "order_id", "orders", "order_id"),
    ("order_details", "product_id", "products", "product_id"),
    ("products", "category_id", "categories", "category_id"),
    ("products", "supplier_id", "suppliers", "supplier_id"),
    ("customer_customer_demo", "customer_id", "customers", "customer_id"),
    ("customer_customer_demo", "customer_type_id", "customer_demographics", "customer_type_id"),
    ("employee_territories", "employee_id", "employees", "employee_id"),
    ("employee_territories", "territory_id", "territories", "territory_id"),
    ("territories", "region_id", "region", "region_id"),
]

fk_rows = []
for child_table, child_column, parent_table, parent_column in FOREIGN_KEYS:
    child = (
        dfs[child_table]
        .select(F.col(child_column).alias("fk"))
        .filter(F.col("fk").isNotNull())
        .distinct()
    )
    parent = dfs[parent_table].select(F.col(parent_column).alias("pk")).distinct()
    orphan_count = child.join(parent, child.fk == parent.pk, "left_anti").count()
    fk_rows.append(
        (child_table, child_column, parent_table, parent_column, orphan_count)
    )

fk_quality = spark.createDataFrame(
    fk_rows,
    ["tabela_origem", "coluna_fk", "tabela_referenciada", "coluna_pk", "chaves_orfas"],
)
display(fk_quality.orderBy(F.desc("chaves_orfas"), "tabela_origem"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Regras de domínio e consistência temporal
# MAGIC
# MAGIC Procuramos valores impossíveis ou suspeitos sem corrigir os dados. As regras são hipóteses de qualidade que devem ser confirmadas com a definição da fonte antes de virarem bloqueios de produção.

# COMMAND ----------

orders = dfs["orders"]
details = dfs["order_details"]
products = dfs["products"]
employees = dfs["employees"]

quality_checks = [
    ("orders", "frete negativo", orders.filter(F.col("freight") < 0)),
    (
        "orders",
        "data requerida anterior à data do pedido",
        orders.filter(
            F.col("order_date").isNotNull()
            & F.col("required_date").isNotNull()
            & (F.col("required_date") < F.col("order_date"))
        ),
    ),
    (
        "orders",
        "envio anterior à data do pedido",
        orders.filter(
            F.col("order_date").isNotNull()
            & F.col("shipped_date").isNotNull()
            & (F.col("shipped_date") < F.col("order_date"))
        ),
    ),
    ("order_details", "preço unitário negativo", details.filter(F.col("unit_price") < 0)),
    ("order_details", "quantidade não positiva", details.filter(F.col("quantity") <= 0)),
    (
        "order_details",
        "desconto fora do intervalo de 0 a 1",
        details.filter((F.col("discount") < 0) | (F.col("discount") > 1)),
    ),
    ("products", "preço de produto negativo", products.filter(F.col("unit_price") < 0)),
    ("products", "estoque negativo", products.filter(F.col("units_in_stock") < 0)),
    (
        "products",
        "indicador discontinued diferente de 0 ou 1",
        products.filter(~F.col("discontinued").isin(0, 1)),
    ),
    (
        "employees",
        "contratação anterior ao nascimento",
        employees.filter(
            F.col("birth_date").isNotNull()
            & F.col("hire_date").isNotNull()
            & (F.col("hire_date") < F.col("birth_date"))
        ),
    ),
]

domain_quality = spark.createDataFrame(
    [(table_name, rule, df.count()) for table_name, rule, df in quality_checks],
    ["tabela", "regra", "linhas_afetadas"],
)
display(domain_quality.orderBy(F.desc("linhas_afetadas"), "tabela"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Cobertura temporal e operação de entrega
# MAGIC
# MAGIC Medimos o período coberto pela base, a concentração mensal de pedidos e os prazos de entrega. Pedidos ainda não enviados ficam fora do cálculo de prazo realizado.

# COMMAND ----------

date_coverage = orders.agg(
    F.min("order_date").alias("primeiro_pedido"),
    F.max("order_date").alias("ultimo_pedido"),
    F.count("order_date").alias("pedidos_com_data"),
    F.countDistinct("order_date").alias("dias_com_pedido"),
    F.sum(F.when(F.col("shipped_date").isNull(), 1).otherwise(0)).alias("pedidos_sem_envio"),
)
display(date_coverage)

monthly_orders = (
    orders.filter(F.col("order_date").isNotNull())
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("mes"))
    .agg(
        F.countDistinct("order_id").alias("pedidos"),
        F.countDistinct("customer_id").alias("clientes_ativos"),
        F.sum("freight").alias("frete_total"),
    )
    .orderBy("mes")
)
display(monthly_orders)

delivery = (
    orders.filter(F.col("shipped_date").isNotNull() & F.col("order_date").isNotNull())
    .withColumn("dias_ate_envio", F.datediff("shipped_date", "order_date"))
    .withColumn("dias_apos_prazo", F.datediff("shipped_date", "required_date"))
)
display(
    delivery.agg(
        F.count("order_id").alias("pedidos_enviados"),
        F.expr("percentile_approx(dias_ate_envio, 0.5)").alias("mediana_dias_ate_envio"),
        F.avg("dias_ate_envio").alias("media_dias_ate_envio"),
        F.sum(F.when(F.col("dias_apos_prazo") > 0, 1).otherwise(0)).alias("pedidos_enviados_atrasados"),
        F.avg(F.when(F.col("dias_apos_prazo").isNotNull(), F.col("dias_apos_prazo"))).alias("media_dias_vs_prazo"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Perfil comercial no grão correto
# MAGIC
# MAGIC Receita bruta e líquida são calculadas por item de pedido. Em seguida agregamos ao grão de pedido antes de juntar a `orders`, evitando multiplicar frete ou pedidos pelo número de itens.

# COMMAND ----------

line_items = (
    details.withColumn("valor_bruto_item", F.col("unit_price") * F.col("quantity"))
    .withColumn("valor_desconto_item", F.col("unit_price") * F.col("quantity") * F.col("discount"))
    .withColumn("valor_liquido_item", F.col("valor_bruto_item") - F.col("valor_desconto_item"))
)

order_values = line_items.groupBy("order_id").agg(
    F.sum("valor_bruto_item").alias("valor_bruto"),
    F.sum("valor_desconto_item").alias("valor_desconto"),
    F.sum("valor_liquido_item").alias("valor_liquido"),
    F.sum("quantity").alias("unidades"),
    F.count(F.lit(1)).alias("itens_no_pedido"),
)

order_facts = orders.join(order_values, "order_id", "left")

business_kpis = order_facts.agg(
    F.countDistinct("order_id").alias("pedidos"),
    F.countDistinct("customer_id").alias("clientes_com_pedido"),
    F.sum("valor_bruto").alias("venda_bruta_itens"),
    F.sum("valor_desconto").alias("descontos_concedidos"),
    F.sum("valor_liquido").alias("venda_liquida_itens"),
    F.sum("unidades").alias("unidades_vendidas"),
    F.avg("valor_liquido").alias("ticket_medio_por_pedido"),
    F.avg("freight").alias("frete_medio_por_pedido"),
    F.avg("itens_no_pedido").alias("itens_medios_por_pedido"),
)
display(business_kpis)

monthly_sales = (
    order_facts.filter(F.col("order_date").isNotNull())
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("mes"))
    .agg(
        F.countDistinct("order_id").alias("pedidos"),
        F.sum("valor_liquido").alias("venda_liquida_itens"),
        F.sum("valor_desconto").alias("descontos"),
        F.sum("unidades").alias("unidades"),
    )
    .orderBy("mes")
)
display(monthly_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Produtos, categorias e descontos
# MAGIC
# MAGIC O ranking evidencia concentração de vendas e permite conferir se os identificadores de produto e categoria estão conciliados. Quantidade vendida e valor líquido respondem a perguntas diferentes e aparecem juntos.

# COMMAND ----------

product_sales = (
    line_items.groupBy("product_id")
    .agg(
        F.sum("quantity").alias("unidades_vendidas"),
        F.sum("valor_bruto_item").alias("venda_bruta"),
        F.sum("valor_desconto_item").alias("descontos"),
        F.sum("valor_liquido_item").alias("venda_liquida"),
        F.countDistinct("order_id").alias("pedidos_com_produto"),
    )
    .join(products.select("product_id", "product_name", "category_id", "discontinued"), "product_id", "left")
    .join(dfs["categories"].select("category_id", "category_name"), "category_id", "left")
)

display(product_sales.orderBy(F.desc("venda_liquida")).limit(20))

category_sales = (
    product_sales.groupBy("category_id", "category_name")
    .agg(
        F.countDistinct("product_id").alias("produtos_vendidos"),
        F.sum("unidades_vendidas").alias("unidades_vendidas"),
        F.sum("venda_liquida").alias("venda_liquida"),
        F.sum("descontos").alias("descontos"),
    )
    .orderBy(F.desc("venda_liquida"))
)
display(category_sales)

discount_bands = (
    line_items.withColumn(
        "faixa_desconto",
        F.when(F.col("discount") == 0, "sem desconto")
        .when(F.col("discount") <= 0.05, "até 5%")
        .when(F.col("discount") <= 0.15, "5% a 15%")
        .otherwise("acima de 15%"),
    )
    .groupBy("faixa_desconto")
    .agg(
        F.count(F.lit(1)).alias("linhas_de_pedido"),
        F.sum("quantity").alias("unidades"),
        F.sum("valor_bruto_item").alias("venda_bruta"),
        F.sum("valor_desconto_item").alias("descontos"),
        F.sum("valor_liquido_item").alias("venda_liquida"),
    )
    .orderBy("faixa_desconto")
)
display(discount_bands)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Clientes, países e força de vendas
# MAGIC
# MAGIC Comparamos clientes cadastrados com clientes que fizeram pedidos e examinamos a distribuição de vendas por país e funcionário. O valor por funcionário é atribuído pelo responsável registrado no pedido e não mede esforço individual nem margem.

# COMMAND ----------

customer_activity = (
    customers.select("customer_id", "company_name", "country")
    .join(
        orders.groupBy("customer_id").agg(
            F.countDistinct("order_id").alias("pedidos"),
            F.min("order_date").alias("primeiro_pedido"),
            F.max("order_date").alias("ultimo_pedido"),
        ),
        "customer_id",
        "left",
    )
)

display(
    customer_activity.agg(
        F.countDistinct("customer_id").alias("clientes_cadastrados"),
        F.countDistinct(F.when(F.col("pedidos") > 0, F.col("customer_id"))).alias("clientes_com_pedido"),
        F.sum(F.when(F.col("pedidos").isNull(), 1).otherwise(0)).alias("clientes_sem_pedido"),
        F.expr("percentile_approx(pedidos, 0.5)").alias("mediana_pedidos_por_cliente_ativo"),
    )
)
display(
    customer_activity.groupBy("country")
    .agg(
        F.countDistinct("customer_id").alias("clientes_cadastrados"),
        F.countDistinct(F.when(F.col("pedidos") > 0, F.col("customer_id"))).alias("clientes_ativos"),
        F.sum("pedidos").alias("pedidos"),
    )
    .orderBy(F.desc("pedidos"))
)

employee_sales = (
    order_facts.groupBy("employee_id")
    .agg(
        F.countDistinct("order_id").alias("pedidos"),
        F.sum("valor_liquido").alias("venda_liquida_itens"),
    )
    .join(
        employees.select("employee_id", "first_name", "last_name", "title", "reports_to"),
        "employee_id",
        "left",
    )
    .orderBy(F.desc("venda_liquida_itens"))
)
display(employee_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Amostras para inspeção e próximos passos
# MAGIC
# MAGIC Use as amostras para conferir a semântica das colunas e os resultados suspeitos encontrados acima. Não conclua causalidade a partir de agregações descritivas. Registre as decisões de qualidade que forem confirmadas e implemente as correções na camada Silver, preservando a bronze como referência da origem.

# COMMAND ----------

display(dfs["orders"].orderBy("order_id").limit(10))
display(dfs["order_details"].orderBy("order_id", "product_id").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Roteiro de leitura dos resultados
# MAGIC
# MAGIC 1. A inventariação confirma presença e volume das 14 tabelas esperadas.
# MAGIC 2. Completude, chaves e integridade referencial mostram onde a origem precisa de tratamento ou documentação.
# MAGIC 3. Regras temporais e de domínio destacam valores impossíveis e hipóteses a validar.
# MAGIC 4. Cobertura temporal e métricas comerciais descrevem o comportamento observado, com o grão explicitado.
# MAGIC 5. Achados confirmados devem virar regras e testes da camada Silver; métricas de negócio e definições devem ser documentadas na Gold.
# MAGIC
# MAGIC **Limites da análise:** a base Northwind é pequena e histórica, não representa necessariamente uma operação atual. A venda líquida aqui desconta apenas o desconto da linha. Não há custo, margem, devolução ou imposto para calcular lucro ou receita contábil.

