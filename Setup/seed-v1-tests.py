# Databricks notebook source
# MAGIC %md
# MAGIC # Seed dos testes do V1
# MAGIC >
# MAGIC > Insere os testes migrados do V1 em tb_test_configurations.
# MAGIC > Dados legiveis abaixo (JSON). Revise em Setup/seed-v1-tests-preview.md.
# MAGIC > 25 ACTIVE + 3 PAUSED. Idempotente (DELETE por test_name + append).

# COMMAND ----------

dbutils.widgets.text("catalog", "sandbox")
dbutils.widgets.text("schema",  "grc")
catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")
dbutils.widgets.dropdown("force_daily", "false", ["false", "true"])
force_daily = dbutils.widgets.get("force_daily") == "true"
target  = f"{catalog}.{schema}.tb_test_configurations"
print("Alvo:", target, "| force_daily:", force_daily)

# COMMAND ----------

import json
TESTS = json.loads(r'''
[
  {
    "test_name": "new-participants-validation",
    "output_table": "tb_incidents_cadastra_new_participants_validation",
    "description": "[Enviar para a Área de Clientes] Este teste possui o objetivo de identificar participantes que possuem empresas com CNPJ criado há menos de um ano, que foram registradas na CERC como participantes, e verifica se a criação é recente e consistente com a atividade da empresa.",
    "responsible_area": "Riscos e Compliance",
    "risco_id": "R313",
    "threshold": 0,
    "frequency": "MONTHLY",
    "query_type": "PYTHON",
    "imports": "",
    "query_code": "first_day_last_month = trunc(add_months(current_date(), -1), \"month\")\nlast_day_last_month = last_day(add_months(current_date(), -1))\n\nestablishment = (\n    spark.table(\"trusted.irs_public_data.tb_establishment\")\n    .withColumnRenamed(\"DocumentCd\", \"DocumentNumberCd\")\n)\n\nparticipants = (\n    spark.table(\"trusted.payment_scheme_record.tb_participant\")\n    .where(col(\"StatusId\") == lit(\"1\"))\n)\n\nfi_registration = (\n    spark.table(\"trusted.cvm_public_data.tb_fi_registration\")\n    .select(\n        col(\"FundDocumentCd\").alias(\"DocumentNumberCd\"),\n        col(\"CvmCd\").alias(\"FiCvmCd\")\n    )\n)\n\nfund_registration = (\n    spark.table(\"trusted.cvm_public_data.tb_fund_registration\")\n    .select(\n        col(\"FundDocumentCd\").alias(\"DocumentNumberCd\"),\n        \"FundRegistrationId\"\n    )\n)\n\ndf_incidents = (\n    participants\n    .where(col(\"CreatedAtDttm\").between(first_day_last_month, last_day_last_month))\n    .join(establishment, \"DocumentNumberCd\", \"left\")\n    .join(fi_registration, \"DocumentNumberCd\", \"left\")\n    .join(fund_registration, \"DocumentNumberCd\", \"left\")\n    .where((col(\"FiCvmCd\").isNull()) & (col(\"FundRegistrationId\").isNull()))\n    .withColumn(\"DaysDifference\", datediff(current_date(), col(\"ActivityStartDt\")))\n    .where(col(\"DaysDifference\") <= lit(365))\n    .select(\n        \"DocumentNumberCd\",\n        \"ParticipantId\",\n        \"ParticipantName\",\n        col(\"ActivityStartDt\").alias(\"CompanyActivityStartDt\"),\n        col(\"CreatedAtDttm\").alias(\"CercCreatedAt\"),\n        \"DaysDifference\"\n    )\n)",
    "category": "cadastra",
    "status": "ACTIVE",
    "should_activate_channel": true
  },
  {
    "test_name": "participants-incorrectly-registered-as-fidc",
    "output_table": "tb_incidents_cadastra_participants_incorrectly_registered_as_fidc",
    "description": "Este teste possui o objetivo de identificar participantes ativos do sistema CERC registrados como FIDC que não apresentam correspondência nos registros oficiais de fundos mantidos pela CVM, configurando uma possível inconsistência cadastral.",
    "responsible_area": "TI Corporativa",
    "risco_id": "R451",
    "threshold": 0,
    "frequency": "MONTHLY",
    "query_type": "PYTHON",
    "imports": "",
    "query_code": "df_participant = (\n    spark.table(\"trusted.payment_scheme_record.tb_participant\")\n    .where(col(\"StatusId\") == \"1\")\n)\n\ndf_fi_registration = (\n    spark.table(\"trusted.cvm_public_data.tb_fi_registration\")\n    .select(\n        col(\"FundDocumentCd\").alias(\"DocumentNumberCd\"),\n        col(\"CvmCd\").alias(\"FiCvmCd\")\n    )\n)\n\ndf_fund_registration = (\n    spark.table(\"trusted.cvm_public_data.tb_fund_registration\")\n    .select(\n        col(\"FundDocumentCd\").alias(\"DocumentNumberCd\"),\n        \"FundRegistrationId\"\n    )\n)\n\ndf_participant_nature_operation = (\n    spark.table(\"trusted.payment_scheme_record.tb_participant_nature_operation\")\n    .where(col(\"StatusId\") == 1)\n    .select(\"ParticipantId\", \"NatureOperationId\")\n)\n\ndf_nature_operation = (\n    spark.table(\"trusted.payment_scheme_record.tb_nature_operation\")\n    .select(\"NatureOperationId\", \"DescriptionDesc\")\n)\n\ndf_joined = (\n    df_participant\n    .join(df_fi_registration, on=\"DocumentNumberCd\", how=\"left\")\n    .join(df_fund_registration, on=\"DocumentNumberCd\", how=\"left\")\n    .join(df_participant_nature_operation, on=\"ParticipantId\", how=\"left\")\n    .join(df_nature_operation, on=\"NatureOperationId\", how=\"left\")\n)\n\ndf_incidents = (\n    df_joined\n    .where((col(\"FiCvmCd\").isNull()) & (col(\"FundRegistrationId\").isNull()))\n    .where(col(\"DescriptionDesc\") == lit(\"FIDC\"))\n    .select(\n        \"DocumentNumberCd\",\n        \"ParticipantId\",\n        \"ParticipantName\",\n        \"NatureOperationId\",\n        \"DescriptionDesc\",\n        \"CreatedAtDttm\",\n        \"FiCvmCd\",\n        \"FundRegistrationId\",\n    )\n)",
    "category": "cadastra",
    "status": "ACTIVE",
    "should_activate_channel": true
  },
  {
    "test_name": "participants-joined-cerc-same-month-as-cnpj-creation",
    "output_table": "tb_incidents_cadastra_participants_joined_cerc_same_month_as_cnpj_creation",
    "description": "Este teste identifica participantes do sistema CERC - com exclusão de FIDCs - cujo CNPJ foi criado e que se tornaram participantes da CERC no mesmo mês, o que pode indicar risco de fraude, registros fictícios ou onboarding sem validação adequada.",
    "responsible_area": "Riscos e Compliance",
    "risco_id": "R313",
    "threshold": 0,
    "frequency": "MONTHLY",
    "query_type": "PYTHON",
    "imports": "",
    "query_code": "first_day_last_month = trunc(add_months(current_date(), -1), \"month\")\nlast_day_last_month = last_day(add_months(current_date(), -1))\n\nestablishment = (\n    spark.table(\"trusted.irs_public_data.tb_establishment\")\n    .withColumnRenamed(\"DocumentCd\", \"DocumentNumberCd\")\n)\n\nparticipants = (\n    spark.table(\"trusted.payment_scheme_record.tb_participant\")\n    .where(col(\"StatusId\") == lit(\"1\"))\n)\n\nfi_registration = (\n    spark.table(\"trusted.cvm_public_data.tb_fi_registration\")\n    .select(\n        col(\"FundDocumentCd\").alias(\"DocumentNumberCd\"),\n        col(\"CvmCd\").alias(\"FiCvmCd\")\n    )\n)\n\nfund_registration = (\n    spark.table(\"trusted.cvm_public_data.tb_fund_registration\")\n    .select(\n        col(\"FundDocumentCd\").alias(\"DocumentNumberCd\"),\n        \"FundRegistrationId\"\n    )\n)\n\ndf_incidents = (\n    participants\n    .where(col(\"CreatedAtDttm\").between(first_day_last_month, last_day_last_month))\n    .join(establishment, \"DocumentNumberCd\", \"left\")\n    .join(fi_registration, \"DocumentNumberCd\", \"left\")\n    .join(fund_registration, \"DocumentNumberCd\", \"left\")\n    .where((col(\"FiCvmCd\").isNull()) & (col(\"FundRegistrationId\").isNull()))\n    .withColumn(\"DaysDifference\", datediff(col(\"CreatedAtDttm\"), col(\"ActivityStartDt\")))\n    .where(col(\"DaysDifference\") <= lit(30))\n    .select(\n        \"DocumentNumberCd\",\n        \"ParticipantId\",\n        \"ParticipantName\",\n        \"FiCvmCd\",\n        \"FundRegistrationId\",\n        \"ActivityStartDt\",\n        \"CreatedAtDttm\",\n        \"DaysDifference\"\n    )\n)",
    "category": "cadastra",
    "status": "ACTIVE",
    "should_activate_channel": true
  },
  {
    "test_name": "participants-with-duplicate-documents",
    "output_table": "tb_incidents_cadastra_participants_with_duplicate_documents",
    "description": "Este teste identifica se há participantes no sistema CERC que compartilham o mesmo CNPJ, verificando a existência de registros duplicados entre diferentes participantes.",
    "responsible_area": "TI Corporativa",
    "risco_id": "R201",
    "threshold": 0,
    "frequency": "MONTHLY",
    "query_type": "PYTHON",
    "imports": "",
    "query_code": "df_incidents = (\n    spark.table(\"trusted.payment_scheme_record.tb_participant\")\n    .where(col(\"StatusId\") == lit(\"1\"))\n    .groupBy(\"DocumentNumberCd\")\n    .agg(countDistinct(\"ParticipantId\").alias(\"CountParticipantId\"))\n    .where(col(\"CountParticipantId\") > lit(\"1\"))\n)",
    "category": "cadastra",
    "status": "ACTIVE",
    "should_activate_channel": true
  },
  {
    "test_name": "participants-with-judicial-sanctions",
    "output_table": "tb_incidents_cadastra_participants_with_judicial_sanctions",
    "description": "Este teste identifica se os participantes do sistema CERC possuem sanções judiciais, incluindo registros no CNEP, CEPIM, CEIS ou em Acordos de Leniência.",
    "responsible_area": "Riscos e Compliance",
    "risco_id": "R313",
    "threshold": 0,
    "frequency": "MONTHLY",
    "query_type": "PYTHON",
    "imports": "",
    "query_code": "participant = (\n    spark.table(\"trusted.payment_scheme_record.tb_participant\")\n    .where(col(\"StatusId\") == lit(\"1\"))\n    .select(\n        \"ParticipantId\",\n        \"DocumentNumberCd\",\n        \"ParticipantName\",\n        \"StatusId\"\n    )\n)\n\nceis = (\n    spark.table(\"trusted.judicial_sanctions_public_data.tb_ceis\")\n    .select(\n        col(\"SanctionedCpfOrCnpjCd\").alias(\"DocumentNumberCd\"),\n        concat_ws(\"-\", col(\"SanctionCodeCd\"), col(\"SanctionCategoryDesc\")).alias(\"CEISIncidence\")\n    )\n)\n\ncepim = (\n    spark.table(\"trusted.judicial_sanctions_public_data.tb_cepim\")\n    .select(\n        col(\"EntityCnpjCd\").alias(\"DocumentNumberCd\"),\n        concat_ws(\"-\", col(\"AgreementNum\"), col(\"GrantingAgencyDesc\")).alias(\"CEPIMIncidence\")\n    )\n)\n\ncnep = (\n    spark.table(\"trusted.judicial_sanctions_public_data.tb_cnep\")\n    .select(\n        col(\"SanctionedCpfOrCnpjCd\").alias(\"DocumentNumberCd\"),\n        concat_ws(\"-\", col(\"SanctionCodeCd\"), col(\"SanctionCategoryDesc\")).alias(\"CNEPIncidence\")\n    )\n)\n\nleniency = (\n    spark.table(\"trusted.judicial_sanctions_public_data.tb_leniency_agreements\")\n    .select(\n        col(\"SanctionedCompanyCnpjCd\").alias(\"DocumentNumberCd\"),\n        concat_ws(\"-\", col(\"ProcessNum\"), col(\"SanctioningOfficeDesc\")).alias(\"LeniencyIncidence\")\n    )\n)\n\ndf_incidents = (\n    participant\n    .join(ceis, \"DocumentNumberCd\", \"left\")\n    .join(cepim, \"DocumentNumberCd\", \"left\")\n    .join(cnep, \"DocumentNumberCd\", \"left\")\n    .join(leniency, \"DocumentNumberCd\", \"left\")\n    .where(\n        (col(\"CEISIncidence\").isNotNull()) |\n        (col(\"CEPIMIncidence\").isNotNull()) |\n        (col(\"CNEPIncidence\").isNotNull()) |\n        (col(\"LeniencyIncidence\").isNotNull())\n    )\n    .dropDuplicates([\"DocumentNumberCd\"])\n)",
    "category": "cadastra",
    "status": "ACTIVE",
    "should_activate_channel": true
  },
  {
    "test_name": "participants-with-unmatched-document-number",
    "output_table": "tb_incidents_cadastra_participants_with_unmatched_document_number",
    "description": "Este teste identifica se há participantes ativos no sistema CERC cujo número de documento (CNPJ) não possui correspondência na base de estabelecimentos da Receita Federal, indicando registros com documentos inválidos ou não cadastrados.",
    "responsible_area": "TI Corporativa",
    "risco_id": "R451",
    "threshold": 0,
    "frequency": "MONTHLY",
    "query_type": "PYTHON",
    "imports": "",
    "query_code": "establishment = (\n    spark.table(\"trusted.irs_public_data.tb_establishment\")\n    .withColumnRenamed(\"DocumentCd\", \"DocumentNumberCd\")\n)\n\nparticipants = (\n    spark.table(\"trusted.payment_scheme_record.tb_participant\")\n    .where(col(\"StatusId\") == lit(\"1\"))\n)\n\ndf_incidents = (\n    participants\n    .join(establishment, \"DocumentNumberCd\", \"left\")\n    .where(col(\"RootDocumentCd\").isNull())\n    .select(\n        \"DocumentNumberCd\",\n        \"ParticipantId\",\n        \"ParticipantName\",\n        \"DocumentTypeId\",\n        \"CreatedAtDttm\",\n        \"CreatedById\",\n        \"RootDocumentCd\"\n    )\n)",
    "category": "cadastra",
    "status": "ACTIVE",
    "should_activate_channel": true
  },
  {
    "test_name": "participants-without-digital-onboarding",
    "output_table": "tb_incidents_cadastra_participants_without_digital_onboarding",
    "description": "Este teste tem como objetivo identificar os participantes ativos do sistema CERC que não passaram pelo processo de onboarding digital.",
    "responsible_area": "TI Corporativa",
    "risco_id": "R393",
    "threshold": 0,
    "frequency": "MONTHLY",
    "query_type": "PYTHON",
    "imports": "",
    "query_code": "first_day_last_month = trunc(add_months(current_date(), -1), \"month\")\nlast_day_last_month = last_day(add_months(current_date(), -1))\n\nestablishment = (\n    spark.table(\"trusted.irs_public_data.tb_establishment\")\n    .withColumnRenamed(\"DocumentCd\", \"DocumentNumberCd\")\n)\n\nparticipants = (\n    spark.table(\"trusted.payment_scheme_record.tb_participant\")\n    .where(col(\"StatusId\") == lit(\"1\"))\n)\n\nfi_registration = (\n    spark.table(\"trusted.cvm_public_data.tb_fi_registration\")\n    .select(\n        col(\"FundDocumentCd\").alias(\"DocumentNumberCd\"),\n        \"FundSocialName\"\n    )\n)\n\nfund_registration = (\n    spark.table(\"trusted.cvm_public_data.tb_fund_registration\")\n    .select(\n        col(\"FundDocumentCd\").alias(\"DocumentNumberCd\"),\n        \"FundRegistrationId\"\n    )\n)\n\ndf_incidents = (\n    participants\n    .where(col(\"CreatedAtDttm\").between(first_day_last_month, last_day_last_month))\n    .join(establishment, \"DocumentNumberCd\", \"left\")\n    .join(fi_registration, \"DocumentNumberCd\", \"left\")\n    .join(fund_registration, \"DocumentNumberCd\", \"left\")\n    .where((col(\"FundSocialName\").isNull()) & (col(\"FundRegistrationId\").isNull()))\n    .withColumn(\"DaysDifference\", datediff(col(\"CreatedAtDttm\"), col(\"ActivityStartDt\")))\n    .where(col(\"DaysDifference\") <= lit(30))\n    .select(\n        \"DocumentNumberCd\",\n        \"ParticipantId\",\n        \"ParticipantName\",\n        \"FundSocialName\",\n        \"FundRegistrationId\",\n        \"ActivityStartDt\",\n        \"CreatedAtDttm\",\n        \"DaysDifference\"\n    )\n)",
    "category": "cadastra",
    "status": "ACTIVE",
    "should_activate_channel": true
  },
  {
    "test_name": "participants-without-valid-cnpj",
    "output_table": "tb_incidents_cadastra_participants_without_valid_cnpj",
    "description": "Este teste identifica se os participantes do sistema CERC possuem CNPJ com situação cadastral ativa na Receita Federal. CNPJs com situação diferente de 'ATIVA' são considerados inválidos.",
    "responsible_area": "Riscos e Compliance",
    "risco_id": "R313",
    "threshold": 0,
    "frequency": "MONTHLY",
    "query_type": "PYTHON",
    "imports": "",
    "query_code": "participant = (\n    spark.table(\"trusted.payment_scheme_record.tb_participant\")\n    .where(col(\"StatusId\") == lit(\"1\"))\n    .select(\n        \"ParticipantId\",\n        \"DocumentNumberCd\",\n        \"ParticipantName\",\n        \"StatusId\"\n    )\n)\n\nestablishment = (\n    spark.table(\"trusted.irs_public_data.tb_establishment\")\n    .select(\n        col(\"DocumentCd\").alias(\"DocumentNumberCd\"),\n        \"RegistrationStatusReasonCd\",\n        \"RegistrationStatusDt\"\n    )\n)\n\nstatus_reason = (\n    spark.table(\"trusted.irs_public_data.tb_registration_status_reason\")\n    .select(\n        \"RegistrationStatusReasonCd\",\n        \"RegistrationStatusReasonDesc\"\n    )\n)\n\nnature_operation = (\n    spark.table(\"trusted.payment_scheme_record.tb_nature_operation\")\n    .where(col(\"StatusCd\") == lit(\"1\"))\n    .select(\n        \"NatureOperationId\",\n        \"DescriptionDesc\"\n    )\n)\n\nparticipant_nature_operation = (\n    spark.table(\"trusted.payment_scheme_record.tb_participant_nature_operation\")\n    .select(\n        \"ParticipantId\",\n        \"NatureOperationId\"\n    )\n    .join(nature_operation, \"NatureOperationId\", \"left\")\n    .groupBy(\"ParticipantId\")\n    .agg(collect_set(\"DescriptionDesc\").alias(\"OperationNatureDesc\"))       \n)\n\ndf_incidents = (\n    participant\n    .join(establishment, \"DocumentNumberCd\", \"left\")\n    .join(status_reason, \"RegistrationStatusReasonCd\", \"left\")\n    .join(participant_nature_operation, \"ParticipantId\", \"left\")\n    .where(col(\"RegistrationStatusReasonCd\") != lit(\"00\"))  # ATIVA\n    .select(\n        \"ParticipantId\",\n        \"DocumentNumberCd\",\n        \"ParticipantName\",\n        \"StatusId\",\n        \"RegistrationStatusDt\",\n        \"OperationNatureDesc\",\n        \"RegistrationStatusReasonCd\",\n        \"RegistrationStatusReasonDesc\"\n    )\n)",
    "category": "cadastra",
    "status": "ACTIVE",
    "should_activate_channel": true
  },
  {
    "test_name": "participants-without-valid-email",
    "output_table": "tb_incidents_cadastra_participants_without_valid_email",
    "description": "Este teste identifica participantes ativos no sistema CERC cujo e-mail institucional contém a palavra “cerc” ou está ausente (nulo), indicando registros com informações de contato inválidas ou incompletas.",
    "responsible_area": "TI Corporativa",
    "risco_id": "R451",
    "threshold": 50,
    "frequency": "MONTHLY",
    "query_type": "PYTHON",
    "imports": "",
    "query_code": "df_incidents = (\n    spark.table(\"trusted.payment_scheme_record.tb_participant\")\n    .where(col(\"StatusId\") == lit(\"1\"))\n    .where(col(\"EmailName\").contains(\"cerc\") | col(\"EmailName\").isNull())\n    .select(\n        \"ParticipantId\",\n        \"DocumentNumberCd\",\n        \"ParticipantName\",\n        \"EmailName\",\n        \"CreatedAtDttm\",\n        \"CreatedById\"\n    )\n)",
    "category": "cadastra",
    "status": "ACTIVE",
    "should_activate_channel": false
  },
  {
    "test_name": "participants-without-valid-phone-number",
    "output_table": "tb_incidents_cadastra_participants_without_valid_phone_number",
    "description": "Este teste identifica participantes do sistema CERC que não possuem um número de telefone válido, considerando como inválidos os casos em que o campo está nulo, vazio, possui menos de 8 dígitos ou contém sequências numéricas repetitivas (como “12345”, “11111”, etc.).",
    "responsible_area": "TI Corporativa",
    "risco_id": "R451",
    "threshold": 50,
    "frequency": "MONTHLY",
    "query_type": "PYTHON",
    "imports": "",
    "query_code": "from functools import reduce\n\ninvalid_phone = (\n    col(\"Phone1Num\").isNull()\n    | (col(\"Phone1Num\") == \"\")\n    | (length(\"Phone1Num\") < 8)\n    | reduce(\n        lambda a, b: a | b,\n        [col(\"Phone1Num\").contains(p) for p in ([\"12345\"] + [str(i) * 5 for i in range(10)])],\n    )\n)\n\ndf_incidents = (\n    spark.table(\"trusted.payment_scheme_record.tb_participant\")\n    .where((col(\"StatusId\") == \"1\") & invalid_phone)\n)",
    "category": "cadastra",
    "status": "ACTIVE",
    "should_activate_channel": false
  },
  {
    "test_name": "users-with-many-distinct-ips-per-day",
    "output_table": "tb_incidents_cadastra_users_with_many_distinct_ips_per_day",
    "description": "Este teste identifica usuários do sistema CERC que acessam a plataforma a partir de muitos endereços IP distintos em um mesmo dia, o que pode indicar compartilhamento indevido de contas, uso de automações, comprometimento de credenciais ou outras atividades suspeitas.",
    "responsible_area": "TI Corporativa",
    "risco_id": "R442",
    "threshold": 0,
    "frequency": "MONTHLY",
    "query_type": "PYTHON",
    "imports": "",
    "query_code": "first_day_last_month = trunc(add_months(current_date(), -1), \"month\")\nlast_day_last_month  = last_day(add_months(current_date(), -1))\n\nbase = (\n    spark.table(\"trusted.payment_scheme_record.tb_historic\")\n    .where(col(\"CreatedAtDttm\").between(first_day_last_month, last_day_last_month))\n    .withColumn(\"Date\", to_date(\"CreatedAtDttm\"))\n    .withColumn(\"Action\", trim(regexp_extract(\"DescriptionTxt\", r\"\\[([^\\]]+)\\]\", 1)))\n    .withColumn(\n        \"UserId\",\n        nullif(\n            trim(regexp_extract(\"DescriptionTxt\", r\"user/participant id:\\s*([^\\]]+)\\]\", 1)),\n            lit(\"\")\n        )\n    )\n    .join(\n        spark.table(\"trusted.payment_scheme_record.tb_user\")\n            .select(\"UserId\", \"UserName\", \"EmailAddressTxt\", \"StatusId\", \"DocumentNumberCd\"),\n        \"UserId\",\n        \"left\"\n    )\n    .dropDuplicates()\n)\n\ndf_incidents = (\n    base.select(\"EmailAddressTxt\", \"UserId\", \"CreatedByIpCd\", \"Date\")\n    .na.drop(subset=[\"EmailAddressTxt\", \"CreatedByIpCd\", \"Date\"])\n    .groupBy(\"EmailAddressTxt\", \"UserId\", \"Date\")\n    .agg(\n        countDistinct(\"CreatedByIpCd\").alias(\"DistinctIPCount\"),\n        collect_set(\"CreatedByIpCd\").alias(\"DistinctIPs\"),\n    )\n    .where(col(\"DistinctIPCount\") > lit(3))\n)",
    "category": "cadastra",
    "status": "ACTIVE",
    "should_activate_channel": true
  },
  {
    "test_name": "cadastra-users-with-sso-bypass",
    "output_table": "tb_incidents_cadastra_users_with_sso_bypass",
    "description": "Este teste identifica os usuários que passaram pela CERC que conseguem acessar o sistema CERC 2.0 sem passar pela autenticação via SSO (Single Sign-On). Esses usuários não estão vinculados ao participante da CERC e, portanto, conseguem acessar o sistema sem realizar o login padrão exigido.",
    "responsible_area": "TI Corporativa",
    "risco_id": "R43",
    "threshold": 0,
    "frequency": "WEEKLY",
    "query_type": "PYTHON",
    "imports": "",
    "query_code": "df_incidents = (\n    spark.table(\"trusted.payment_scheme_record.tb_user\")\n    .where(\n        (col(\"StatusId\") == 1) &\n        lower(col(\"EmailAddressTxt\")).rlike(\"@(cerc|venhapranuvem|vericode)\")\n    )\n    .select(\"UserId\", \"UserName\", \"EmailAddressTxt\")\n    .join(\n        spark.table(\"trusted.payment_scheme_record.tb_user_company_profile\").select(\"UserId\", \"CompanyId\"),\n        \"UserId\",\n        \"left\"\n    )\n    .join(\n        spark.table(\"trusted.payment_scheme_record.tb_company\").select(\"CompanyId\", \"CorporateName\"),\n        \"CompanyId\",\n        \"left\"\n    )\n    .groupBy(\"UserId\")\n    .agg(\n        first(\"EmailAddressTxt\", True).alias(\"EmailAddressTxt\"),\n        first(\"UserName\", True).alias(\"UserName\"),\n        collect_set(\"CorporateName\").alias(\"CompanyList\"),\n        max(when(lower(col(\"CorporateName\")).contains(\"cerc\"), 1).otherwise(0)).alias(\"HasCerc\")\n    )\n    .where(col(\"HasCerc\") == 0)\n    .select(\"UserId\", \"EmailAddressTxt\", \"UserName\", \"CompanyList\")\n)",
    "category": "cadastra",
    "status": "ACTIVE",
    "should_activate_channel": true
  },
  {
    "test_name": "ccb-dc-data-quality-relevant-timelines-after-tech-updates-10-dec-2025",
    "output_table": "tb_incidents_cedulas_ccb_dc_data_quality_relevant_timelines_after_tech_updates_10_dec_2025",
    "description": "Diagnosticar reincidência da inconsistência de registros de CCBs e DCs. Esse teste considera o cenário de CCBs e DCs com data de vencimento antes da data de registro para novos registros, após correções aplicadas em 10-Dez-2025 pelos times de TI da CERC para corrigir de forma definitiva esse problema.",
    "responsible_area": "Recebíveis Comerciais​",
    "risco_id": "R1472",
    "threshold": 0,
    "frequency": "MONTHLY",
    "query_type": "PYTHON",
    "imports": "",
    "query_code": "#Lógica do teste - CENÁRIO NOVOS REGISTROS (após 10-Dez-2025)\n\nfrom pyspark.sql.functions import col\nimport traceback\n\ndf_incidents = spark.sql(\"\"\"\n\nWITH parsed AS (\n  SELECT\n    AssetId,\n    TypeAssetId,\n    CASE TypeAssetId\n      WHEN 1  THEN 'CCB'  WHEN 2  THEN 'CPR'  WHEN 3  THEN 'CDCA' WHEN 4  THEN 'ACC'\n      WHEN 5  THEN 'CCCM' WHEN 6  THEN 'CCE'  WHEN 7  THEN 'CCES' WHEN 8  THEN 'CCIN'\n      WHEN 9  THEN 'CDAWA' WHEN 10 THEN 'CDIV' WHEN 11 THEN 'CMER' WHEN 12 THEN 'CMUT'\n      WHEN 13 THEN 'CRH'  WHEN 14 THEN 'CRP'  WHEN 15 THEN 'CRPH' WHEN 16 THEN 'DEB'\n      WHEN 17 THEN 'DUPR' WHEN 18 THEN 'NC'   WHEN 19 THEN 'NCCM' WHEN 20 THEN 'NCE'\n      WHEN 21 THEN 'NCIN' WHEN 22 THEN 'NCR'  WHEN 23 THEN 'CPR_VERDE' WHEN 24 THEN 'NOTA_PROMISSORIA'\n      WHEN 25 THEN 'DUPLICATAS' WHEN 26 THEN 'DUPLICATA_MERCANTIL_CERC' WHEN 27 THEN 'DUPLICATA_SERVICO_CERC'\n      WHEN 28 THEN 'CHEQUE_CERC' WHEN 29 THEN 'CONTRATO_CERC' WHEN 30 THEN 'NOTA_PROMISSORIA_CERC'\n      WHEN 31 THEN 'CONTRATO_DIREITO_CREDITORIO_CERC' WHEN 32 THEN 'NOTA_FISCAL_ELETRONICA_CERC'\n      WHEN 33 THEN 'CIR' WHEN 34 THEN 'CRI' WHEN 35 THEN 'CRA' WHEN 36 THEN 'CR'\n      WHEN 37 THEN 'LCI' WHEN 38 THEN 'LCA' WHEN 39 THEN 'DC'  WHEN 40 THEN 'CCI'\n      WHEN 41 THEN 'CCCB' WHEN 999 THEN 'OUTROS'\n      ELSE CAST(TypeAssetId AS STRING)\n    END AS TypeAssetName,\n    StatusAssetId,\n    RegistryDttm,\n    DueDt,\n\n    COALESCE(\n      DATE(RegistryDttm),\n      to_date(cast(RegistryDttm AS string), 'yyyy-MM-dd'),\n      to_date(cast(RegistryDttm AS string), 'dd/MM/yyyy'),\n      to_date(cast(RegistryDttm AS string), 'dd-MM-yyyy')\n    ) AS RegistryDate_parsed,\n\n    COALESCE(\n      DATE(DueDt),\n      to_date(cast(DueDt AS string), 'yyyy-MM-dd'),\n      to_date(cast(DueDt AS string), 'dd/MM/yyyy'),\n      to_date(cast(DueDt AS string), 'dd-MM-yyyy')\n    ) AS DueDate_parsed\n\n  FROM trusted.banking_credit_notes.tb_asset\n  WHERE TypeAssetId IN (1, 39) -- CCB, DC\n)\n\nSELECT\n  AssetId,\n  TypeAssetId,\n  TypeAssetName,\n  StatusAssetId,\n  RegistryDttm       AS Registry_orig,\n  DueDt              AS Due_orig,\n  RegistryDate_parsed,\n  DueDate_parsed,\n  datediff(DueDate_parsed, RegistryDate_parsed) AS Days_Due_minus_Registry,\n  'DEPOIS 10-Dez-2025' AS moment\n\nFROM parsed\nWHERE\n  RegistryDate_parsed IS NOT NULL\n  AND DueDate_parsed IS NOT NULL\n  AND datediff(DueDate_parsed, RegistryDate_parsed) < 0 -- Due_date antes de Registry_date (problema)\n  AND RegistryDate_parsed > to_date('2025-12-10')\n\n\n\"\"\")",
    "category": "cedulas",
    "status": "ACTIVE",
    "should_activate_channel": true
  },
  {
    "test_name": "ccb-dc-data-quality-relevant-timelines-before-tech-updates-10-dec-2025",
    "output_table": "tb_incidents_cedulas_ccb_dc_data_quality_relevant_timelines_before_tech_updates_10_dec_2025",
    "description": "Diagnosticar inconsistência de registros de CPRs. Esse teste considera o cenário de CPRs com data de vencimento antes da data de registro para o estoque dos registros, antes das correções aplicadas em 10-Dez-2025 pelos times de TI da CERC para corrigir de forma definitiva esse problema. Impacto regulatório dado acordo realizado pela CERC com o Banco Central do Brasil em 23-Out-2025 por meio de Termo de Compromisso para estar em conformidade com a Instrução Normativa n° 520 de 6-Set-2024.",
    "responsible_area": "Projetos Não-Regulatórios",
    "risco_id": "R1472",
    "threshold": 0,
    "frequency": "MONTHLY",
    "query_type": "PYTHON",
    "imports": "",
    "query_code": "#Lógica do teste - CENÁRIO ESTOQUE (até 10-Dez-2025)\n\nfrom pyspark.sql.functions import col\nimport traceback\n\ndf_incidents = spark.sql(\"\"\"\n\nWITH parsed AS (\n  SELECT\n    AssetId,\n    TypeAssetId,\n    CASE TypeAssetId\n      WHEN 1  THEN 'CCB'  WHEN 2  THEN 'CPR'  WHEN 3  THEN 'CDCA' WHEN 4  THEN 'ACC'\n      WHEN 5  THEN 'CCCM' WHEN 6  THEN 'CCE'  WHEN 7  THEN 'CCES' WHEN 8  THEN 'CCIN'\n      WHEN 9  THEN 'CDAWA' WHEN 10 THEN 'CDIV' WHEN 11 THEN 'CMER' WHEN 12 THEN 'CMUT'\n      WHEN 13 THEN 'CRH'  WHEN 14 THEN 'CRP'  WHEN 15 THEN 'CRPH' WHEN 16 THEN 'DEB'\n      WHEN 17 THEN 'DUPR' WHEN 18 THEN 'NC'   WHEN 19 THEN 'NCCM' WHEN 20 THEN 'NCE'\n      WHEN 21 THEN 'NCIN' WHEN 22 THEN 'NCR'  WHEN 23 THEN 'CPR_VERDE' WHEN 24 THEN 'NOTA_PROMISSORIA'\n      WHEN 25 THEN 'DUPLICATAS' WHEN 26 THEN 'DUPLICATA_MERCANTIL_CERC' WHEN 27 THEN 'DUPLICATA_SERVICO_CERC'\n      WHEN 28 THEN 'CHEQUE_CERC' WHEN 29 THEN 'CONTRATO_CERC' WHEN 30 THEN 'NOTA_PROMISSORIA_CERC'\n      WHEN 31 THEN 'CONTRATO_DIREITO_CREDITORIO_CERC' WHEN 32 THEN 'NOTA_FISCAL_ELETRONICA_CERC'\n      WHEN 33 THEN 'CIR' WHEN 34 THEN 'CRI' WHEN 35 THEN 'CRA' WHEN 36 THEN 'CR'\n      WHEN 37 THEN 'LCI' WHEN 38 THEN 'LCA' WHEN 39 THEN 'DC'  WHEN 40 THEN 'CCI'\n      WHEN 41 THEN 'CCCB' WHEN 999 THEN 'OUTROS'\n      ELSE CAST(TypeAssetId AS STRING)\n    END AS TypeAssetName,\n    StatusAssetId,\n    RegistryDttm,\n    DueDt,\n\n    COALESCE(\n      DATE(RegistryDttm),\n      to_date(cast(RegistryDttm AS string), 'yyyy-MM-dd'),\n      to_date(cast(RegistryDttm AS string), 'dd/MM/yyyy'),\n      to_date(cast(RegistryDttm AS string), 'dd-MM-yyyy')\n    ) AS RegistryDate_parsed,\n\n    COALESCE(\n      DATE(DueDt),\n      to_date(cast(DueDt AS string), 'yyyy-MM-dd'),\n      to_date(cast(DueDt AS string), 'dd/MM/yyyy'),\n      to_date(cast(DueDt AS string), 'dd-MM-yyyy')\n    ) AS DueDate_parsed\n\n  FROM trusted.banking_credit_notes.tb_asset\n  WHERE TypeAssetId IN (1, 39) -- CCB, DC\n)\n\nSELECT\n  AssetId,\n  TypeAssetId,\n  TypeAssetName,\n  StatusAssetId,\n  RegistryDttm       AS Registry_orig,\n  DueDt              AS Due_orig,\n  RegistryDate_parsed,\n  DueDate_parsed,\n  datediff(DueDate_parsed, RegistryDate_parsed) AS Days_Due_minus_Registry,\n  'ATE_10-Dez-2025' AS moment\n\nFROM parsed\nWHERE\n  RegistryDate_parsed IS NOT NULL\n  AND DueDate_parsed IS NOT NULL\n  AND datediff(DueDate_parsed, RegistryDate_parsed) < 0 -- Due_date antes de Registry_date (problema)\n  AND RegistryDate_parsed <= DATE('2025-12-10')\n\n\"\"\")",
    "category": "cedulas",
    "status": "ACTIVE",
    "should_activate_channel": true
  },
  {
    "test_name": "cpr-data-quality-relevant-timelines-after-tech-updates-10-dec-2025",
    "output_table": "tb_incidents_cedulas_cpr_data_quality_relevant_timelines_after_tech_updates_10_dec_2025",
    "description": "Diagnosticar reincidência da inconsistência de registros de CPRs. Esse teste considera o cenário de CPRs com data de vencimento antes da data de registro para novos registros, após correções aplicadas em 10-Dez-2025 pelos times de TI da CERC para corrigir de forma definitiva esse problema. Impacto regulatório dado acordo realizado pela CERC com o Banco Central do Brasil em 23-Out-2025 por meio de Termo de Compromisso para estar em conformidade com a Instrução Normativa n° 520 de 6-Set-2024.",
    "responsible_area": "Engenharia de Produtos",
    "risco_id": "R1470",
    "threshold": 0,
    "frequency": "MONTHLY",
    "query_type": "PYTHON",
    "imports": "",
    "query_code": "#Lógica do teste - CENÁRIO NOVOS REGISTROS (após 10-Dez-2025)\n\nfrom pyspark.sql.functions import col\nimport traceback\n\ndf_incidents = spark.sql(\"\"\"\n\nWITH parsed AS (\n  SELECT\n    AssetId,\n    TypeAssetId,\n    CASE TypeAssetId\n      WHEN 1  THEN 'CCB'  WHEN 2  THEN 'CPR'  WHEN 3  THEN 'CDCA' WHEN 4  THEN 'ACC'\n      WHEN 5  THEN 'CCCM' WHEN 6  THEN 'CCE'  WHEN 7  THEN 'CCES' WHEN 8  THEN 'CCIN'\n      WHEN 9  THEN 'CDAWA' WHEN 10 THEN 'CDIV' WHEN 11 THEN 'CMER' WHEN 12 THEN 'CMUT'\n      WHEN 13 THEN 'CRH'  WHEN 14 THEN 'CRP'  WHEN 15 THEN 'CRPH' WHEN 16 THEN 'DEB'\n      WHEN 17 THEN 'DUPR' WHEN 18 THEN 'NC'   WHEN 19 THEN 'NCCM' WHEN 20 THEN 'NCE'\n      WHEN 21 THEN 'NCIN' WHEN 22 THEN 'NCR'  WHEN 23 THEN 'CPR_VERDE' WHEN 24 THEN 'NOTA_PROMISSORIA'\n      WHEN 25 THEN 'DUPLICATAS' WHEN 26 THEN 'DUPLICATA_MERCANTIL_CERC' WHEN 27 THEN 'DUPLICATA_SERVICO_CERC'\n      WHEN 28 THEN 'CHEQUE_CERC' WHEN 29 THEN 'CONTRATO_CERC' WHEN 30 THEN 'NOTA_PROMISSORIA_CERC'\n      WHEN 31 THEN 'CONTRATO_DIREITO_CREDITORIO_CERC' WHEN 32 THEN 'NOTA_FISCAL_ELETRONICA_CERC'\n      WHEN 33 THEN 'CIR' WHEN 34 THEN 'CRI' WHEN 35 THEN 'CRA' WHEN 36 THEN 'CR'\n      WHEN 37 THEN 'LCI' WHEN 38 THEN 'LCA' WHEN 39 THEN 'DC'  WHEN 40 THEN 'CCI'\n      WHEN 41 THEN 'CCCB' WHEN 999 THEN 'OUTROS'\n      ELSE CAST(TypeAssetId AS STRING)\n    END AS TypeAssetName,\n    StatusAssetId,\n    RegistryDttm,\n    DueDt,\n\n    COALESCE(\n      DATE(RegistryDttm),\n      to_date(cast(RegistryDttm AS string), 'yyyy-MM-dd'),\n      to_date(cast(RegistryDttm AS string), 'dd/MM/yyyy'),\n      to_date(cast(RegistryDttm AS string), 'dd-MM-yyyy')\n    ) AS RegistryDate_parsed,\n\n    COALESCE(\n      DATE(DueDt),\n      to_date(cast(DueDt AS string), 'yyyy-MM-dd'),\n      to_date(cast(DueDt AS string), 'dd/MM/yyyy'),\n      to_date(cast(DueDt AS string), 'dd-MM-yyyy')\n    ) AS DueDate_parsed\n\n  FROM trusted.banking_credit_notes.tb_asset\n  WHERE TypeAssetId = 2 -- CPRs\n)\n\nSELECT\n  AssetId,\n  TypeAssetId,\n  TypeAssetName,\n  StatusAssetId,\n  RegistryDttm       AS Registry_orig,\n  DueDt              AS Due_orig,\n  RegistryDate_parsed,\n  DueDate_parsed,\n  datediff(DueDate_parsed, RegistryDate_parsed) AS Days_Due_minus_Registry,\n  'DEPOIS 10-Dez-2025' AS moment\n\nFROM parsed\nWHERE\n  RegistryDate_parsed IS NOT NULL\n  AND DueDate_parsed IS NOT NULL\n  AND datediff(DueDate_parsed, RegistryDate_parsed) < 0 -- Due_date antes de Registry_date (problema)\n  AND RegistryDate_parsed > to_date('2025-12-10')\n\n\n\"\"\")",
    "category": "cedulas",
    "status": "ACTIVE",
    "should_activate_channel": true
  },
  {
    "test_name": "cpr-data-quality-relevant-timelines-before-tech-updates-10-dec-2025",
    "output_table": "tb_incidents_cedulas_cpr_data_quality_relevant_timelines_before_tech_updates_10_dec_2025",
    "description": "Diagnosticar inconsistência de registros de CPRs. Esse teste considera o cenário de CPRs com data de vencimento antes da data de registro para o estoque dos registros, antes das correções aplicadas em 10-Dez-2025 pelos times de TI da CERC para corrigir de forma definitiva esse problema. Impacto regulatório dado acordo realizado pela CERC com o Banco Central do Brasil em 23-Out-2025 por meio de Termo de Compromisso para estar em conformidade com a Instrução Normativa n° 520 de 6-Set-2024.",
    "responsible_area": "Monitoramento de Mercado",
    "risco_id": "R1473",
    "threshold": 0,
    "frequency": "MONTHLY",
    "query_type": "PYTHON",
    "imports": "",
    "query_code": "#Lógica do teste - CENÁRIO ESTOQUE (até 10-Dez-2025)\n\nfrom pyspark.sql.functions import col\nimport traceback\n\ndf_incidents = spark.sql(\"\"\"\n\nWITH parsed AS (\n  SELECT\n    AssetId,\n    TypeAssetId,\n    CASE TypeAssetId\n      WHEN 1  THEN 'CCB'  WHEN 2  THEN 'CPR'  WHEN 3  THEN 'CDCA' WHEN 4  THEN 'ACC'\n      WHEN 5  THEN 'CCCM' WHEN 6  THEN 'CCE'  WHEN 7  THEN 'CCES' WHEN 8  THEN 'CCIN'\n      WHEN 9  THEN 'CDAWA' WHEN 10 THEN 'CDIV' WHEN 11 THEN 'CMER' WHEN 12 THEN 'CMUT'\n      WHEN 13 THEN 'CRH'  WHEN 14 THEN 'CRP'  WHEN 15 THEN 'CRPH' WHEN 16 THEN 'DEB'\n      WHEN 17 THEN 'DUPR' WHEN 18 THEN 'NC'   WHEN 19 THEN 'NCCM' WHEN 20 THEN 'NCE'\n      WHEN 21 THEN 'NCIN' WHEN 22 THEN 'NCR'  WHEN 23 THEN 'CPR_VERDE' WHEN 24 THEN 'NOTA_PROMISSORIA'\n      WHEN 25 THEN 'DUPLICATAS' WHEN 26 THEN 'DUPLICATA_MERCANTIL_CERC' WHEN 27 THEN 'DUPLICATA_SERVICO_CERC'\n      WHEN 28 THEN 'CHEQUE_CERC' WHEN 29 THEN 'CONTRATO_CERC' WHEN 30 THEN 'NOTA_PROMISSORIA_CERC'\n      WHEN 31 THEN 'CONTRATO_DIREITO_CREDITORIO_CERC' WHEN 32 THEN 'NOTA_FISCAL_ELETRONICA_CERC'\n      WHEN 33 THEN 'CIR' WHEN 34 THEN 'CRI' WHEN 35 THEN 'CRA' WHEN 36 THEN 'CR'\n      WHEN 37 THEN 'LCI' WHEN 38 THEN 'LCA' WHEN 39 THEN 'DC'  WHEN 40 THEN 'CCI'\n      WHEN 41 THEN 'CCCB' WHEN 999 THEN 'OUTROS'\n      ELSE CAST(TypeAssetId AS STRING)\n    END AS TypeAssetName,\n    StatusAssetId,\n    RegistryDttm,\n    DueDt,\n\n    COALESCE(\n      DATE(RegistryDttm),\n      to_date(cast(RegistryDttm AS string), 'yyyy-MM-dd'),\n      to_date(cast(RegistryDttm AS string), 'dd/MM/yyyy'),\n      to_date(cast(RegistryDttm AS string), 'dd-MM-yyyy')\n    ) AS RegistryDate_parsed,\n\n    COALESCE(\n      DATE(DueDt),\n      to_date(cast(DueDt AS string), 'yyyy-MM-dd'),\n      to_date(cast(DueDt AS string), 'dd/MM/yyyy'),\n      to_date(cast(DueDt AS string), 'dd-MM-yyyy')\n    ) AS DueDate_parsed\n\n  FROM trusted.banking_credit_notes.tb_asset\n  WHERE TypeAssetId = 2 -- CPRs\n)\n\nSELECT\n  AssetId,\n  TypeAssetId,\n  TypeAssetName,\n  StatusAssetId,\n  RegistryDttm       AS Registry_orig,\n  DueDt              AS Due_orig,\n  RegistryDate_parsed,\n  DueDate_parsed,\n  datediff(DueDate_parsed, RegistryDate_parsed) AS Days_Due_minus_Registry,\n  'ATE_10-Dez-2025' AS moment\n\nFROM parsed\nWHERE\n  RegistryDate_parsed IS NOT NULL\n  AND DueDate_parsed IS NOT NULL\n  AND datediff(DueDate_parsed, RegistryDate_parsed) < 0 -- Due_date antes de Registry_date (problema)\n  AND RegistryDate_parsed <= DATE('2025-12-10')\n\n\"\"\")",
    "category": "cedulas",
    "status": "PAUSED",
    "should_activate_channel": true
  },
  {
    "test_name": "index-below-regulatory-requirement",
    "output_table": "tb_incidents_cerc_system_availability_index_below_regulatory_requirement",
    "description": "O objetivo deste teste é verificar se, do ponto de vista do cliente, a disponibilidade do Sistema CERC pelos canais HTTPS (Portal e APIs) de cada sistema e endpoint se manteve acima do índice mínimo de 99,8%, identificando dias em que o indicador de disponibilidade caiu abaixo desse patamar.",
    "responsible_area": "Infraestrutura",
    "risco_id": "R294",
    "threshold": 0,
    "frequency": "DAILY",
    "query_type": "PYTHON",
    "imports": "",
    "query_code": "df_incidents = (\n    spark.table(\"trusted.infracorp.tb_availability_http\")\n    .where(col(\"EventDt\") == date_sub(current_date(), 1))\n    .groupBy(\n        col(\"EventDt\"),\n        col(\"ProjectId\"),\n        col(\"EndpointTxt\")\n    )\n    .agg(\n        sum(col(\"RequestCountQty\")).alias(\"total_requests\"),\n        sum(\n            when(col(\"HttpStatusCd\") >= 500, col(\"RequestCountQty\")).otherwise(0)\n        ).alias(\"failed_requests\")\n    )\n    .withColumn(\n        \"availability_pct\",\n        (lit(1) - (col(\"failed_requests\") / col(\"total_requests\"))) * lit(100)\n    )\n    .where(col(\"availability_pct\") < 99.8)\n)",
    "category": "cerc-system-availability",
    "status": "PAUSED",
    "should_activate_channel": true
  },
  {
    "test_name": "claude-token-audit-log",
    "output_table": "tb_claude_audit_log_claude_token_audit_log",
    "description": "Detecta criacao/delecao de token e extracao explicita de dados via eventos de exportacao no Activity Feed da Compliance API (ultimos 3 dias).",
    "responsible_area": "Arquitetura",
    "risco_id": "R1474",
    "threshold": 0,
    "frequency": "DAILY",
    "query_type": "PYTHON",
    "imports": "",
    "query_code": "# Exemplo: Lógica do teste\nimport json\nimport time\nimport traceback\nfrom datetime import datetime, timedelta, timezone\nfrom typing import Any\n\nimport requests\nfrom pyspark.sql.types import StringType, StructField, StructType\n\nAPI_KEY = dbutils.secrets.get(scope=\"compliance-grc\", key=\"claude-audit-api-key\")\nAPI_BASE = \"https://api.anthropic.com/v1/compliance\"\nAPI_VERSION = \"2026-03-29\"\n\nLOOKBACK_DAYS = 3\nORG_PAGE_LIMIT = 1000\nACTIVITY_PAGE_LIMIT = 1000\nMAX_ORG_PAGES = 100\nMAX_ACTIVITY_PAGES_PER_ORG = 200\nMAX_RETRIES = 4\n\nTOKEN_CREATED_OR_DELETED_TYPES = {\n    \"api_key_created\",\n    \"api_key_deleted\",\n    \"admin_api_key_created\",\n    \"admin_api_key_deleted\",\n    \"platform_api_key_created\",\n    \"platform_api_key_deleted\",\n    \"scoped_api_key_deleted\",\n}\n\nEXPLICIT_EXPORT_TYPES = {\n    \"org_data_export_started\",\n    \"org_data_export_completed\",\n    \"org_members_exported\",\n    \"student_data_exported\",\n}\n\nend_at = datetime.now(timezone.utc).replace(microsecond=0)\nstart_at = end_at - timedelta(days=LOOKBACK_DAYS)\ncreated_at_gte = start_at.isoformat().replace(\"+00:00\", \"Z\")\ncreated_at_lte = end_at.isoformat().replace(\"+00:00\", \"Z\")\n\nsession = requests.Session()\nsession.headers.update(\n    {\n        \"x-api-key\": API_KEY,\n        \"anthropic-version\": API_VERSION,\n        \"user-agent\": \"ContinuousAuditTokenExportTest/1.0\",\n    }\n)\n\ndef compliance_get(path: str, params: list[tuple[str, str]] | None = None) -> dict[str, Any]:\n    url = f\"{API_BASE.rstrip('/')}/{path.lstrip('/')}\"\n    for attempt in range(MAX_RETRIES + 1):\n        response = session.get(url, params=params, timeout=60)\n        if response.status_code == 200:\n            return response.json()\n\n        if response.status_code in {408, 409, 425, 429, 500, 502, 503, 504} and attempt < MAX_RETRIES:\n            time.sleep(min(0.5 * (2**attempt), 8.0))\n            continue\n\n        raise RuntimeError(f\"Compliance API error ({response.status_code}) in {path}: {response.text[:400]}\")\n\n    raise RuntimeError(f\"Failed after retries: {path}\")\n\ndef list_all_organizations() -> list[dict[str, Any]]:\n    organizations: list[dict[str, Any]] = []\n    params: list[tuple[str, str]] = [(\"limit\", str(ORG_PAGE_LIMIT))]\n\n    for _ in range(MAX_ORG_PAGES):\n        payload = compliance_get(\"/organizations\", params=params)\n        data = payload.get(\"data\", [])\n        if isinstance(data, list):\n            organizations.extend(item for item in data if isinstance(item, dict))\n\n        has_more = bool(payload.get(\"has_more\"))\n        next_page = payload.get(\"next_page\")\n        last_id = payload.get(\"last_id\")\n\n        if has_more and next_page:\n            params = [(k, v) for k, v in params if k != \"page\"]\n            params.append((\"page\", str(next_page)))\n            continue\n        if has_more and last_id:\n            params = [(k, v) for k, v in params if k not in {\"after_id\", \"before_id\"}]\n            params.append((\"after_id\", str(last_id)))\n            continue\n        break\n\n    dedup: dict[str, dict[str, Any]] = {}\n    for org in organizations:\n        org_uuid = org.get(\"uuid\") or org.get(\"id\")\n        if org_uuid:\n            dedup[str(org_uuid)] = org\n    return list(dedup.values())\n\ndef list_org_activities(org_uuid: str) -> list[dict[str, Any]]:\n    params: list[tuple[str, str]] = [\n        (\"organization_ids[]\", org_uuid),\n        (\"created_at.gte\", created_at_gte),\n        (\"created_at.lte\", created_at_lte),\n        (\"limit\", str(ACTIVITY_PAGE_LIMIT)),\n    ]\n\n    events: list[dict[str, Any]] = []\n    current = list(params)\n    for _ in range(MAX_ACTIVITY_PAGES_PER_ORG):\n        payload = compliance_get(\"/activities\", params=current)\n        data = payload.get(\"data\", [])\n        if isinstance(data, list):\n            events.extend(item for item in data if isinstance(item, dict))\n\n        has_more = bool(payload.get(\"has_more\"))\n        last_id = payload.get(\"last_id\")\n        if not has_more or not last_id:\n            break\n\n        current = [(k, v) for k, v in current if k not in {\"after_id\", \"before_id\"}]\n        current.append((\"after_id\", str(last_id)))\n\n    return events\n\ndef actor_identifier(event: dict[str, Any]) -> str:\n    actor = event.get(\"actor\") if isinstance(event.get(\"actor\"), dict) else {}\n    return (\n        str(actor.get(\"email_address\") or \"\")\n        or str(actor.get(\"user_id\") or \"\")\n        or str(actor.get(\"api_key_id\") or \"\")\n        or str(actor.get(\"id\") or \"\")\n        or \"unknown\"\n    )\n\ndef token_reference(event: dict[str, Any]) -> str:\n    return str(event.get(\"api_key_id\") or event.get(\"admin_api_key_id\") or event.get(\"token_id\") or \"\")\n\ndef build_incident(rule_id: str, event: dict[str, Any], evidence: str) -> dict[str, Any]:\n    return {\n        \"incident_id\": f\"{rule_id}::{event.get('id')}\",\n        \"rule_id\": rule_id,\n        \"event_created_at\": str(event.get(\"created_at\") or \"\"),\n        \"activity_id\": str(event.get(\"id\") or \"\"),\n        \"activity_type\": str(event.get(\"type\") or \"\"),\n        \"organization_uuid\": str(event.get(\"organization_uuid\") or event.get(\"organization_id\") or \"\"),\n        \"actor_identifier\": actor_identifier(event),\n        \"token_reference\": token_reference(event),\n        \"evidence\": evidence,\n        \"raw_event\": json.dumps(event, ensure_ascii=False, default=str),\n    }\n\norganizations = list_all_organizations()\norg_ids = [str(org.get(\"uuid\") or org.get(\"id\")) for org in organizations if org.get(\"uuid\") or org.get(\"id\")]\n\nall_events: list[dict[str, Any]] = []\nfor org_id in org_ids:\n    all_events.extend(list_org_activities(org_id))\n\nincidents: list[dict[str, Any]] = []\nfor event in all_events:\n    event_type = str(event.get(\"type\") or \"\").lower()\n\n    if event_type in TOKEN_CREATED_OR_DELETED_TYPES:\n        incidents.append(\n            build_incident(\n                rule_id=\"token_criado_ou_deletado\",\n                event=event,\n                evidence=\"Evento de criacao/delecao de token/chave.\",\n            )\n        )\n        continue\n\n    if event_type in EXPLICIT_EXPORT_TYPES:\n        incidents.append(\n            build_incident(\n                rule_id=\"extracao_dados_por_exportacao\",\n                event=event,\n                evidence=\"Evento explicito de exportacao de dados.\",\n            )\n        )\n\ndedup: dict[str, dict[str, Any]] = {}\nfor incident in incidents:\n    dedup[str(incident[\"incident_id\"])] = incident\nincidents = list(dedup.values())\n\nincident_schema = StructType(\n    [\n        StructField(\"incident_id\", StringType(), True),\n        StructField(\"rule_id\", StringType(), True),\n        StructField(\"event_created_at\", StringType(), True),\n        StructField(\"activity_id\", StringType(), True),\n        StructField(\"activity_type\", StringType(), True),\n        StructField(\"organization_uuid\", StringType(), True),\n        StructField(\"actor_identifier\", StringType(), True),\n        StructField(\"token_reference\", StringType(), True),\n        StructField(\"evidence\", StringType(), True),\n        StructField(\"raw_event\", StringType(), True),\n    ]\n)\n\ndf_incidents = spark.createDataFrame(incidents, schema=incident_schema) if incidents else spark.createDataFrame([], schema=incident_schema)",
    "category": "claude-audit-log",
    "status": "ACTIVE",
    "should_activate_channel": true
  },
  {
    "test_name": "employees-with-other-active-companies",
    "output_table": "tb_incidents_compliance_employees_with_other_active_companies",
    "description": "Esse teste possui o objetivo de identificar os funcionários da CERC que também constam como sócios em empresas ativas no sistema do Publica, cruzando informações do cadastro interno de colaboradores com os registros públicos de empresas e sócios. O teste permite detectar possíveis conflitos de interesse ou sobreposição de funções entre a atuação como empregado e participação societária em empresas externas.",
    "responsible_area": "Riscos e Compliance",
    "risco_id": "R358",
    "threshold": 0,
    "frequency": "MONTHLY",
    "query_type": "PYTHON",
    "imports": "",
    "query_code": "from functools import reduce\n\nexceptions = [\n    (\"MARIO MELLO FREIRE NETO\", \"BANCO J. SAFRA S.A\"),\n    (\"FATIMA CAROLINA GILBERTO RIOS PEREIRA\", \"ANTECIPA S/A\"),\n    (\"LEONARDO BORGES DA SILVA MARTINS\", \"AKKWA SERVIÇOS FINANCEIROS E TECNOLÓGICOS LTDA\"),\n    (\"PAULO IONESCU\", \"ANTECIPA S/A\"),\n    (\"MARIO MELLO FREIRE NETO\", \"BANCO SAFRA\"),\n]\n\nexceptions_upper = [(name.upper(), company.upper()) for name, company in exceptions]\n\ndf_employees = (\n    spark.table(\"compliance.cerc_data.tb_hr_employees\")\n    .select(\n        \"CorporateEmail\",\n        upper(col(\"EmployeeName\")).alias(\"AssociateName\"),\n        \"DepartmentName\",\n        \"ManagementClassName\",\n        \"HireDate\",\n    )\n)\n\ndf_qsa = (\n    spark.table(\"trusted.irs_public_data.tb_associate\")\n    .select(\n        upper(col(\"AssociateName\")).alias(\"AssociateName\"),\n        \"AssociateQualificationCd\",\n        \"AssociateDocumentCd\",\n        \"RootDocumentCd\",\n    )\n)\n\ndf_company = (\n    spark.table(\"trusted.irs_public_data.tb_company\")\n    .select(\"RootDocumentCd\", \"CompanyName\")\n)\n\ndf_establishment = (\n    spark.table(\"trusted.irs_public_data.tb_establishment\")\n    .select(\"RootDocumentCd\", \"DocumentCd\")\n)\n\ndf_participants = (\n    spark.table(\"trusted.payment_scheme_record.tb_participant\")\n    .where(col(\"StatusId\") == lit(\"1\"))\n    .select(\n        \"ParticipantId\",\n        col(\"DocumentNumberCd\").alias(\"DocumentCd\"),\n        upper(col(\"ParticipantName\")).alias(\"ParticipantName\"),\n        \"CreatedAtDttm\",\n        \"CreatedById\",\n    )\n)\n\ndf_joined = (\n    df_employees\n    .join(df_qsa, on=\"AssociateName\", how=\"left\")\n    .join(df_company, on=\"RootDocumentCd\", how=\"left\")\n    .join(df_establishment, on=\"RootDocumentCd\", how=\"left\")\n    .join(df_participants, on=\"DocumentCd\", how=\"left\")\n    .where(col(\"ParticipantId\").isNotNull())\n    .where(col(\"DocumentCd\") != lit(\"23399607000191\")) \n)\n\nexception_conditions = [\n    (col(\"AssociateName\") == lit(name)) & (col(\"ParticipantName\") == lit(company))\n    for name, company in exceptions_upper\n]\n\ndf_incidents = df_joined.where(~(reduce(lambda a, b: a | b, exception_conditions, lit(False))))",
    "category": "compliance",
    "status": "ACTIVE",
    "should_activate_channel": true
  },
  {
    "test_name": "mandatory-trainings",
    "output_table": "tb_incidents_compliance_mandatory_trainings",
    "description": "Esse teste possui o objetivo de identificar os colaboradores que não realizaram os treinamentos de conformidade obrigatórios: Gestão de Riscos na CERC Guia Basico para Colaboradores e Conformidade e Ética: Guia Básico para Colaboradores.",
    "responsible_area": "CSC",
    "risco_id": "R347",
    "threshold": 0,
    "frequency": "DAILY",
    "query_type": "PYTHON",
    "imports": "",
    "query_code": "employees = (\n    spark.table(\"sandbox.grc.tb_external_hr_employees\")\n    .withColumn(\"Email\", lower(col(\"CorporateEmail\")))\n    .where(col(\"AdmissionDate\") >= lit(\"2024-12-24\"))\n    .select(\"Email\", \"Name\", \"AdmissionDate\", \"DepartmentName\")\n)\n\nknowbe4 = spark.table(\"sandbox.grc.tb_external_knowbe4_compliance_training\")\n\ndf_incidents = (\nemployees\n.join(knowbe4, \"Email\", \"left\")\n.withColumn(\"DateDifference\", datediff(current_timestamp(), col(\"AdmissionDate\")))\n.where((col(\"DateDifference\") >= lit(30)) & (col(\"CompletionDate\").isNull()))\n.select(\n    \"Email\",\n    \"Name\",\n    \"AdmissionDate\", \n    \"DepartmentName\",\n    \"ModuleName\",\n    \"StartDate\",\n    \"CompletionDate\",\n    \"Status\",\n    \"DateDifference\"\n)\n)",
    "category": "compliance",
    "status": "PAUSED",
    "should_activate_channel": true
  },
  {
    "test_name": "access-control-list-changes",
    "output_table": "tb_incidents_databricks_config_access_control_list_changes",
    "description": "Este teste possui o objetivo de realizar uma verificação de todas as alterações feitas na Access Control List (ACL) no dia anterior - Representa qualquer modificação na política que define quem pode criar, listar ou revogar tokens de API dentro do Databricks. Ele identifica quais usuários executaram mudanças, quantas modificações foram realizadas e quais recursos foram impactados.",
    "responsible_area": "Engenharia e Tecnologia KYP",
    "risco_id": "R1463",
    "threshold": 0,
    "frequency": "DAILY",
    "query_type": "PYTHON",
    "imports": "",
    "query_code": "df_incidents = (\n    spark.table(\"system.access.audit\")\n    .where(\n        (col(\"action_name\") == lit(\"changeDbTokenAcl\")) &\n        (col(\"event_date\") == date_sub(current_date(), 1))\n    )\n    .groupBy(\"user_identity.email\")\n    .agg(\n        count(\"event_id\").alias(\"count\"),\n        collect_set(\"account_id\").alias(\"account_id\"),\n        collect_set(\"request_id\").alias(\"request_id\"),\n        collect_set(\"event_id\").alias(\"event_id\"),\n        collect_set(\"event_time\").alias(\"event_time\"),\n        collect_list(\"request_params\").alias(\"request_params\"),\n        collect_list(\"response\").alias(\"response\")\n    )\n)",
    "category": "databricks-config",
    "status": "ACTIVE",
    "should_activate_channel": true
  },
  {
    "test_name": "access-control-list-disable",
    "output_table": "tb_incidents_databricks_config_access_control_list_disable",
    "description": "Este teste possui o objetivo de verificar quando a Access Control List (ACL) foi desativada no Databricks. A ação disableTableAcls representa a remoção temporária ou permanente das regras de controle de acesso que definem quem pode criar, listar ou revogar tokens de API em uma tabela ou recurso específico.",
    "responsible_area": "Engenharia e Tecnologia KYP",
    "risco_id": "R1463",
    "threshold": 0,
    "frequency": "DAILY",
    "query_type": "PYTHON",
    "imports": "",
    "query_code": "df_incidents = (\n    spark.table(\"system.access.audit\")\n    .where(\n        (col(\"action_name\") == lit(\"disableTableAcls\")) &\n        (col(\"event_date\") == date_sub(current_date(), 1))\n    )\n)",
    "category": "databricks-config",
    "status": "ACTIVE",
    "should_activate_channel": true
  },
  {
    "test_name": "download-large-results",
    "output_table": "tb_incidents_databricks_config_download_large_results_v2",
    "description": "Este teste identifica usuários que realizaram o download dos resultados de grandes queries em ambiente de produção dentro do workspace da CERC, o que pode indicar risco de vazamento de dados, uso indevido de informações sensíveis ou descumprimento de políticas de segurança.",
    "responsible_area": "Engenharia e Tecnologia KYP",
    "risco_id": "R271",
    "threshold": 0,
    "frequency": "DAILY",
    "query_type": "PYTHON",
    "imports": "",
    "query_code": "from pyspark.sql.functions import *\nimport traceback\n\n\nbase = spark.table(\"system.access.audit\").alias(\"a\")\n\ndownload_events = (\n    base\n    .filter(col(\"a.action_name\") == \"downloadLargeResults\")\n    .select(\n        col(\"a.event_id\").cast(\"string\").alias(\"event_id\"),\n        col(\"a.request_id\").cast(\"string\").alias(\"request_id\"),\n        col(\"a.session_id\").cast(\"string\").alias(\"session_id\"),\n        col(\"a.event_time\").alias(\"event_time\"),\n        col(\"a.user_identity\").alias(\"download_user_identity\"),\n        col(\"a.service_name\").cast(\"string\").alias(\"service_name\"),\n        col(\"a.action_name\").cast(\"string\").alias(\"action_name\"),\n        col(\"a.request_params\")[\"notebookId\"].cast(\"string\").alias(\"notebook_id\"),\n        col(\"a.request_params\")[\"commandId\"].cast(\"string\").alias(\"command_id\")\n    )\n)\n\nqualified_users = (\n    download_events\n    .withColumn(\"event_day\", to_date(col(\"event_time\")))\n    .filter(col(\"event_day\") == date_sub(current_date(), 1))\n    .groupBy(\"download_user_identity\", \"event_day\")\n    .agg(count(\"*\").alias(\"download_count\"))\n    .filter(col(\"download_count\") >= 5)\n)\n\ndownload_events_filtered = (\n    download_events.alias(\"d\")\n    .join(\n        qualified_users.alias(\"q\"),\n        on=[\n            col(\"d.download_user_identity\") == col(\"q.download_user_identity\"),\n            to_date(col(\"d.event_time\")) == col(\"q.event_day\")\n        ],\n        how=\"inner\"\n    )\n    .select(\n        col(\"d.event_id\"),\n        col(\"d.request_id\"),\n        col(\"d.session_id\"),\n        col(\"d.event_time\"),\n        col(\"d.download_user_identity\"),\n        col(\"d.notebook_id\"),\n        col(\"d.command_id\")\n    )\n)\n\ncommand_events = (\n    base\n    .filter(\n        (col(\"a.service_name\") == \"notebook\") &\n        (col(\"a.action_name\").isin(\"runCommand\", \"submitCommand\")) &\n        col(\"a.request_params\")[\"commandText\"].isNotNull()\n    )\n    .select(\n        col(\"a.request_params\")[\"notebookId\"].cast(\"string\").alias(\"notebook_id\"),\n        col(\"a.request_params\")[\"commandId\"].cast(\"string\").alias(\"command_id\"),\n        col(\"a.request_params\")[\"commandText\"].cast(\"string\").alias(\"statement_text\")\n    )\n    .dropDuplicates([\"notebook_id\", \"command_id\"])\n)\n\ndf_incidents = (\n    download_events_filtered.alias(\"d\")\n    .join(\n        command_events.alias(\"c\"),\n        on=[\"notebook_id\", \"command_id\"],\n        how=\"left\"\n    )\n    .withColumn(\n        \"event_time\",\n        concat(\n            date_format(\n                from_utc_timestamp(col(\"event_time\"), \"America/Sao_Paulo\"),\n                \"dd-MMM-yyyy hh:mm:ss a\"\n            ),\n            lit(\" GMT-3\")\n        )\n    )\n    .select(\n        \"event_time\",\n        \"download_user_identity\",\n        \"statement_text\",\n        \"notebook_id\",\n        \"command_id\",\n        \"request_id\",\n        \"event_id\",\n        \"session_id\"\n    )\n    .orderBy(col(\"event_time\").asc())\n)",
    "category": "databricks-config",
    "status": "ACTIVE",
    "should_activate_channel": true
  },
  {
    "test_name": "external-credentials-creation",
    "output_table": "tb_incidents_databricks_config_external_credentials_creation",
    "description": "Este teste possui o objetivo de verificar todas as ações de criação de credenciais externas registradas no audit log no dia anterior. Ele identifica quais usuários criaram credenciais e garante o monitoramento de operações sensíveis que podem conceder acesso a external locations, storage, conexões e integrações externas.",
    "responsible_area": "Engenharia e Tecnologia KYP",
    "risco_id": "R1463",
    "threshold": 0,
    "frequency": "DAILY",
    "query_type": "PYTHON",
    "imports": "",
    "query_code": "df_incidents = (\n    spark.table(\"system.access.audit\")\n    .where(\n        (col(\"action_name\") == lit(\"createCredential\")) &\n        (col(\"event_date\") == date_sub(current_date(), 1))\n    )\n)",
    "category": "databricks-config",
    "status": "ACTIVE",
    "should_activate_channel": true
  },
  {
    "test_name": "unrevoked-tokens",
    "output_table": "tb_incidents_databricks_config_unrevoked_tokens",
    "description": "Este teste possui o objetivo de verificar todos os tokens gerados no ambiente e identificar aqueles que permanecem ativos sem terem sido revogados. Ele cruza os registros de geração e revogação de tokens presentes no audit log, garantindo o monitoramento de credenciais sensíveis que ainda estão válidas e podem conceder/obter acessos indevidos no sistema.",
    "responsible_area": "Engenharia e Tecnologia KYP",
    "risco_id": "R1463",
    "threshold": 0,
    "frequency": "MONTHLY",
    "query_type": "PYTHON",
    "imports": "",
    "query_code": "revokeDbToken = (\n    spark.table(\"system.access.audit\")\n        .where(col(\"action_name\") == lit(\"revokeDbToken\"))\n        .select(\n            col(\"request_params.tokenHash\").alias(\"token_hash\"),\n            col(\"event_date\").alias(\"revoke_date\")\n        )\n)\n\ndf_incidents = (\n    spark.table(\"system.access.audit\")\n        .where(col(\"action_name\") == lit(\"generateDbToken\"))\n        .withColumn(\n            \"token_expiration_dt\",\n            from_unixtime(col(\"request_params.tokenExpirationTime\") / 1000).cast(\"timestamp\")\n        )\n        .withColumn(\"token_hash\", col(\"request_params.tokenHash\"))\n        .join(revokeDbToken, \"token_hash\", \"left\")\n        .join(spark.table(\"system.access.workspaces_latest\").select(\"workspace_id\", \"workspace_name\", \"workspace_url\"), on=\"workspace_id\", how=\"left\")\\\n        .where(\n            (col(\"revoke_date\").isNull()) &\n            (current_date() <= col(\"token_expiration_dt\")) &\n            (col(\"workspace_name\").contains(\"prd\"))\n        )\n)",
    "category": "databricks-config",
    "status": "ACTIVE",
    "should_activate_channel": true
  },
  {
    "test_name": "update-permission-assignment",
    "output_table": "tb_incidents_databricks_config_update_permission_assignment",
    "description": "Este teste possui o objetivo de verificar todas as ações de atualização de permissões registradas no audit log, excluindo alterações realizadas por identidades de automação, como contas de terraform. Ele identifica quais usuários efetuaram modificações em permissões e garante o monitoramento de operações sensíveis que podem impactar o controle de acesso no ambiente.",
    "responsible_area": "Engenharia e Tecnologia KYP",
    "risco_id": "R446",
    "threshold": 0,
    "frequency": "MONTHLY",
    "query_type": "PYTHON",
    "imports": "",
    "query_code": "df_incidents = (\n    spark.table(\"system.access.audit\")\n        .where(col(\"action_name\") == lit(\"updatePermissionAssignment\"))\n        .where(~col(\"user_identity.email\").contains(\"terraform\"))\n        .where((col(\"event_date\") == date_sub(current_date(), 1)))\n)",
    "category": "databricks-config",
    "status": "ACTIVE",
    "should_activate_channel": true
  },
  {
    "test_name": "anomalous-feeder-consumption-behavior",
    "output_table": "tb_incidents_finance_anomalous_feeder_consumption_behavior",
    "description": "O teste busca detectar padrões anômalos no consumo de feeders da SERPRO pelos clientes, sinalizando aumentos relevantes de consumo que possam representar elevação do risco de crédito.",
    "responsible_area": "Tesouraria",
    "risco_id": "R252",
    "threshold": 0,
    "frequency": "WEEKLY",
    "query_type": "PYTHON",
    "imports": "",
    "query_code": "from pyspark.sql.window import Window\n\ntb_assessments = spark.table(\"trusted.asset_assessment.tb_assessments\")\ntb_external_search = spark.table(\"trusted.asset_assessment.tb_external_search\")\n\nbase = (\n    tb_assessments.alias(\"ass\")\n    .join(\n        tb_external_search.alias(\"ex\"),\n        col(\"ex.AssessmentId\") == col(\"ass.AssessmentId\"),\n        \"left\"\n    )\n    .where(\n        (col(\"ex.UrlParsedName\") != \"cache\") &\n        (col(\"ex.DataContractId\").isin(\n            \"serpro_fetch_cnpj\",\n            \"serpro_fetch_cnpj_pre_contract\",\n            \"serpro_fetch_cpf\",\n            \"serpro_fetch_cpfs\",\n            \"serpro_fetch_cpf_pre_contract\",\n            \"serpro_fetch_cpf_emissor\",\n            \"serpro_fetch_nfe\",\n            \"serpro_fetch_nfe_summary\",\n            \"serpro_fetch_nfe_full\"\n        )) &\n        (dayofweek(col(\"ass.CreatedAtDttm\")).between(2, 6))\n    )\n    .select(\n        col(\"ass.ClientCd\").alias(\"ClientCd\"),\n        col(\"ass.CreatedAtDttm\").alias(\"CreatedAtDttm\"),\n        when(\n            col(\"ex.DataContractId\").isin(\n                \"serpro_fetch_cpf\",\n                \"serpro_fetch_cpfs\",\n                \"serpro_fetch_cpf_pre_contract\",\n                \"serpro_fetch_cpf_emissor\"\n            ),\n            \"CPF\"\n        )\n        .when(\n            col(\"ex.DataContractId\").isin(\n                \"serpro_fetch_cnpj\",\n                \"serpro_fetch_cnpj_pre_contract\"\n            ),\n            \"CNPJ\"\n        )\n        .when(\n            col(\"ex.DataContractId\").isin(\n                \"serpro_fetch_nfe\",\n                \"serpro_fetch_nfe_summary\",\n                \"serpro_fetch_nfe_full\"\n            ),\n            \"NFE\"\n        )\n        .alias(\"AssessmentTypeCd\")\n    )\n)\n\nbase_semana = (\n    base.withColumn(\n        \"CreatedAtDttm\",\n        next_day(\n            date_trunc(\"week\", col(\"CreatedAtDttm\")),\n            \"FRI\"\n        )\n    )\n)\n\nresultado = (\n    base_semana\n    .groupBy(\"ClientCd\", \"CreatedAtDttm\", \"AssessmentTypeCd\")\n    .agg(count(\"*\").alias(\"AssessmentQty\"))\n    .withColumn(\n        \"AssessmentValueAmt\",\n        col(\"AssessmentQty\") *\n        when(col(\"AssessmentTypeCd\") == \"CPF\", 0.13)\n        .when(col(\"AssessmentTypeCd\") == \"CNPJ\", 0.0990)\n        .when(col(\"AssessmentTypeCd\") == \"NFE\", 0.0293)\n        .otherwise(0)\n    )\n)\n\nresultado_final = (\n    resultado\n    .filter(col(\"ClientCd\").isNotNull())\n    .groupBy(\"ClientCd\", \"CreatedAtDttm\")\n    .agg(\n        sum(\"AssessmentValueAmt\").alias(\"AssessmentTotalValueAmt\")\n    )\n)\n\nw = Window.partitionBy(\"ClientCd\").orderBy(\"CreatedAtDttm\")\nw_pico = w.rowsBetween(Window.unboundedPreceding, -1)\n\ntabela_picos = (\n    resultado_final\n    .withColumn(\n        \"PrevPeakValueAmt\",\n        max(\"AssessmentTotalValueAmt\").over(w_pico)\n    )\n    .withColumn(\n        \"PrevPeakValueAmt\",\n        when(\n            col(\"PrevPeakValueAmt\").isNull(),\n            col(\"AssessmentTotalValueAmt\")\n        ).otherwise(col(\"PrevPeakValueAmt\"))\n    )\n    .withColumn(\n        \"VariationPct\",\n        (col(\"AssessmentTotalValueAmt\") - col(\"PrevPeakValueAmt\")) /\n        col(\"PrevPeakValueAmt\")\n    )\n    .withColumn(\n        \"AlertInd\",\n        (col(\"VariationPct\") >= 0.25) &\n        (col(\"AssessmentTotalValueAmt\") >= 25000)\n    )\n)\n\n\ntotal_avaliacoes = (\n    resultado\n    .groupBy(\"ClientCd\", \"CreatedAtDttm\")\n    .agg(\n        sum(\"AssessmentQty\").alias(\"AssessmentTotalQty\")\n    )\n)\n\ndf_incidents = (\n    tabela_picos\n    .filter(col(\"AlertInd\") == True)\n    .filter(\n        col(\"CreatedAtDttm\") == next_day(\n            date_trunc(\"week\", current_date()),\n            \"FRI\"\n        )\n    )\n    .join(\n        total_avaliacoes,\n        [\"ClientCd\", \"CreatedAtDttm\"],\n        \"left\"\n    )\n    .select(\n        \"ClientCd\",\n        \"CreatedAtDttm\",\n        \"AssessmentTotalValueAmt\",\n        \"AssessmentTotalQty\"\n    )\n    .orderBy(\"ClientCd\", \"CreatedAtDttm\")\n)",
    "category": "finance",
    "status": "ACTIVE",
    "should_activate_channel": true
  },
  {
    "test_name": "bypass-in-sonar-testing-policies",
    "output_table": "tb_incidents_policy_devops_bypass_in_sonar_testing_policies",
    "description": "O objetivo deste teste é identificar, quem desativou ou deletou regras (policy) de verificação de código no Sonar. Essas regras funcionam como “proteções automáticas”: quando são removidas, o sistema pode deixar de sinalizar erros de código e/ou falhas de segurança, aumentando o risco de códigos problemáticos seguirem adiante sem o devido controle.",
    "responsible_area": "Infraestrutura",
    "risco_id": "R292",
    "threshold": 0,
    "frequency": "DAILY",
    "query_type": "PYTHON",
    "imports": "",
    "query_code": "df_incidents = (\n    spark.table(\"trusted.infracorp.tb_sonar_policy_history\")\n    .withColumn(\n        \"event_date\",\n        to_date(col(\"IngestedAtDttm\"))\n    )\n    .where(\n        (col(\"ActionName\").isin(\"disabled\", \"deleted\")) &\n        (col(\"event_date\") == date_sub(current_date(), 1))\n    )\n    .groupBy(\n        col(\"UserEmailTxt\"),\n        col(\"ProjectName\"),\n        col(\"RepositoryName\")\n    )\n    .agg(\n        sum(\n            when(col(\"ActionName\") == \"disabled\", 1).otherwise(0)\n        ).alias(\"disabled_count\"),\n        sum(\n            when(col(\"ActionName\") == \"deleted\", 1).otherwise(0)\n        ).alias(\"deleted_count\")\n    )\n)",
    "category": "policy-devops",
    "status": "ACTIVE",
    "should_activate_channel": true
  }
]
''')
print(len(TESTS), "testes |",
      sum(1 for t in TESTS if t["status"]=="ACTIVE"), "ACTIVE |",
      sum(1 for t in TESTS if t["status"]=="PAUSED"), "PAUSED")

# COMMAND ----------

import uuid
from datetime import datetime
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, TimestampType, BooleanType)
now = datetime.now()
schema_def = StructType([
    StructField("test_id", StringType()), StructField("test_name", StringType()),
    StructField("output_table", StringType()), StructField("description", StringType()),
    StructField("responsible_area", StringType()), StructField("risco_id", StringType()),
    StructField("threshold", IntegerType()), StructField("frequency", StringType()),
    StructField("query_type", StringType()), StructField("imports", StringType()),
    StructField("query_code", StringType()), StructField("status", StringType()),
    StructField("category", StringType()), StructField("created_by", StringType()),
    StructField("reviewed_by", StringType()), StructField("rejection_reason", StringType()),
    StructField("created_at", TimestampType()), StructField("updated_at", TimestampType()),
    StructField("activated_at", TimestampType()), StructField("version", IntegerType()),
    StructField("should_activate_channel", BooleanType()),
])
rows = [(
    str(uuid.uuid4()), t["test_name"], t["output_table"], t["description"],
    t["responsible_area"], t["risco_id"], int(t["threshold"]),
    ("DAILY" if force_daily else t["frequency"]),
    t["query_type"], t["imports"], t["query_code"], t["status"], t["category"],
    "v1-migration", None, None, now, now,
    (now if t["status"]=="ACTIVE" else None), 1, bool(t["should_activate_channel"]),
) for t in TESTS]
df = spark.createDataFrame(rows, schema_def)
display(df.select("test_name","category","status","frequency","threshold","should_activate_channel"))

# COMMAND ----------

# Idempotente: remove versoes anteriores destes testes e reinsere
names = [t["test_name"] for t in TESTS]
in_list = ",".join("'" + n.replace("'", "''") + "'" for n in names)
spark.sql(f"DELETE FROM {target} WHERE test_name IN ({in_list})")
df.write.mode("append").saveAsTable(target)
print(f"OK: {len(names)} testes semeados em {target}")

# COMMAND ----------

display(spark.sql(f"SELECT status, count(*) AS n FROM {target} GROUP BY status ORDER BY status"))
