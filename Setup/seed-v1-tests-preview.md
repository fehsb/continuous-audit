# Seed V1 — Preview dos testes migrados

**28 testes** — 25 ACTIVE, 3 PAUSED. Gerado a partir de `Repo CA old/notebooks/automated-tests`. Todos `query_type=PYTHON`, `created_by=v1-migration`.

| # | test_name | categoria | status | freq | thr | notifica | output_table |
|---|---|---|---|---|---|---|---|
| 1 | cadastra-users-with-sso-bypass | cadastra | ACTIVE | WEEKLY | 0 | sim | tb_incidents_cadastra_users_with_sso_bypass |
| 2 | new-participants-validation | cadastra | ACTIVE | MONTHLY | 0 | sim | tb_incidents_cadastra_new_participants_validation |
| 3 | participants-incorrectly-registered-as-fidc | cadastra | ACTIVE | MONTHLY | 0 | sim | tb_incidents_cadastra_participants_incorrectly_registered_as_fidc |
| 4 | participants-joined-cerc-same-month-as-cnpj-creation | cadastra | ACTIVE | MONTHLY | 0 | sim | tb_incidents_cadastra_participants_joined_cerc_same_month_as_cnpj_creation |
| 5 | participants-with-duplicate-documents | cadastra | ACTIVE | MONTHLY | 0 | sim | tb_incidents_cadastra_participants_with_duplicate_documents |
| 6 | participants-with-judicial-sanctions | cadastra | ACTIVE | MONTHLY | 0 | sim | tb_incidents_cadastra_participants_with_judicial_sanctions |
| 7 | participants-with-unmatched-document-number | cadastra | ACTIVE | MONTHLY | 0 | sim | tb_incidents_cadastra_participants_with_unmatched_document_number |
| 8 | participants-without-digital-onboarding | cadastra | ACTIVE | MONTHLY | 0 | sim | tb_incidents_cadastra_participants_without_digital_onboarding |
| 9 | participants-without-valid-cnpj | cadastra | ACTIVE | MONTHLY | 0 | sim | tb_incidents_cadastra_participants_without_valid_cnpj |
| 10 | participants-without-valid-email | cadastra | ACTIVE | MONTHLY | 50 | NÃO | tb_incidents_cadastra_participants_without_valid_email |
| 11 | participants-without-valid-phone-number | cadastra | ACTIVE | MONTHLY | 50 | NÃO | tb_incidents_cadastra_participants_without_valid_phone_number |
| 12 | users-with-many-distinct-ips-per-day | cadastra | ACTIVE | MONTHLY | 0 | sim | tb_incidents_cadastra_users_with_many_distinct_ips_per_day |
| 13 | ccb-dc-data-quality-relevant-timelines-after-tech-updates-10-dec-2025 | cedulas | ACTIVE | MONTHLY | 0 | sim | tb_incidents_cedulas_ccb_dc_data_quality_relevant_timelines_after_tech_updates_10_dec_2025 |
| 14 | ccb-dc-data-quality-relevant-timelines-before-tech-updates-10-dec-2025 | cedulas | ACTIVE | MONTHLY | 0 | sim | tb_incidents_cedulas_ccb_dc_data_quality_relevant_timelines_before_tech_updates_10_dec_2025 |
| 15 | cpr-data-quality-relevant-timelines-after-tech-updates-10-dec-2025 | cedulas | ACTIVE | MONTHLY | 0 | sim | tb_incidents_cedulas_cpr_data_quality_relevant_timelines_after_tech_updates_10_dec_2025 |
| 16 | claude-token-audit-log | claude-audit-log | ACTIVE | DAILY | 0 | sim | tb_incidents_claude_audit_log_claude_token_audit_log |
| 17 | employees-with-other-active-companies | compliance | ACTIVE | MONTHLY | 0 | sim | tb_incidents_compliance_employees_with_other_active_companies |
| 18 | access-control-list-changes | databricks-config | ACTIVE | DAILY | 0 | sim | tb_incidents_databricks_config_access_control_list_changes |
| 19 | access-control-list-disable | databricks-config | ACTIVE | DAILY | 0 | sim | tb_incidents_databricks_config_access_control_list_disable |
| 20 | download-large-results | databricks-config | ACTIVE | DAILY | 0 | sim | tb_incidents_databricks_config_download_large_results_v2 |
| 21 | external-credentials-creation | databricks-config | ACTIVE | DAILY | 0 | sim | tb_incidents_databricks_config_external_credentials_creation |
| 22 | unrevoked-tokens | databricks-config | ACTIVE | MONTHLY | 0 | sim | tb_incidents_databricks_config_unrevoked_tokens |
| 23 | update-permission-assignment | databricks-config | ACTIVE | MONTHLY | 0 | sim | tb_incidents_databricks_config_update_permission_assignment |
| 24 | anomalous-feeder-consumption-behavior | finance | ACTIVE | WEEKLY | 0 | sim | tb_incidents_finance_anomalous_feeder_consumption_behavior |
| 25 | bypass-in-sonar-testing-policies | policy-devops | ACTIVE | DAILY | 0 | sim | tb_incidents_policy_devops_bypass_in_sonar_testing_policies |
| 26 | cpr-data-quality-relevant-timelines-before-tech-updates-10-dec-2025 | cedulas | PAUSED | MONTHLY | 0 | sim | tb_incidents_cedulas_cpr_data_quality_relevant_timelines_before_tech_updates_10_dec_2025 |
| 27 | index-below-regulatory-requirement | cerc-system-availability | PAUSED | DAILY | 0 | sim | tb_incidents_cerc_system_availability_index_below_regulatory_requirement |
| 28 | mandatory-trainings | compliance | PAUSED | DAILY | 0 | sim | tb_incidents_compliance_mandatory_trainings |

---

## Queries completas

### cadastra-users-with-sso-bypass  
`cadastra` · **ACTIVE** · WEEKLY · threshold=0 · risco=R43 · área=TI Corporativa  

> Este teste identifica os usuários que passaram pela CERC que conseguem acessar o sistema CERC 2.0 sem passar pela autenticação via SSO (Single Sign-On). Esses usuários não estão vinculados ao participante da CERC e, portanto, conseguem acessar o sistema sem realizar o login padrão exigido.

```python
df_incidents = (
    spark.table("trusted.payment_scheme_record.tb_user")
    .where(
        (col("StatusId") == 1) &
        lower(col("EmailAddressTxt")).rlike("@(cerc|venhapranuvem|vericode)")
    )
    .select("UserId", "UserName", "EmailAddressTxt")
    .join(
        spark.table("trusted.payment_scheme_record.tb_user_company_profile").select("UserId", "CompanyId"),
        "UserId",
        "left"
    )
    .join(
        spark.table("trusted.payment_scheme_record.tb_company").select("CompanyId", "CorporateName"),
        "CompanyId",
        "left"
    )
    .groupBy("UserId")
    .agg(
        first("EmailAddressTxt", True).alias("EmailAddressTxt"),
        first("UserName", True).alias("UserName"),
        collect_set("CorporateName").alias("CompanyList"),
        max(when(lower(col("CorporateName")).contains("cerc"), 1).otherwise(0)).alias("HasCerc")
    )
    .where(col("HasCerc") == 0)
    .select("UserId", "EmailAddressTxt", "UserName", "CompanyList")
)

df_incidents.display()
```

### new-participants-validation  
`cadastra` · **ACTIVE** · MONTHLY · threshold=0 · risco=R313 · área=Riscos e Compliance  

> [Enviar para a Área de Clientes] Este teste possui o objetivo de identificar participantes que possuem empresas com CNPJ criado há menos de um ano, que foram registradas na CERC como participantes, e verifica se a criação é recente e consistente com a atividade da empresa.

```python
first_day_last_month = trunc(add_months(current_date(), -1), "month")
last_day_last_month = last_day(add_months(current_date(), -1))

establishment = (
    spark.table("trusted.irs_public_data.tb_establishment")
    .withColumnRenamed("DocumentCd", "DocumentNumberCd")
)

participants = (
    spark.table("trusted.payment_scheme_record.tb_participant")
    .where(col("StatusId") == lit("1"))
)

fi_registration = (
    spark.table("trusted.cvm_public_data.tb_fi_registration")
    .select(
        col("FundDocumentCd").alias("DocumentNumberCd"),
        col("CvmCd").alias("FiCvmCd")
    )
)

fund_registration = (
    spark.table("trusted.cvm_public_data.tb_fund_registration")
    .select(
        col("FundDocumentCd").alias("DocumentNumberCd"),
        "FundRegistrationId"
    )
)

df_incidents = (
    participants
    .where(col("CreatedAtDttm").between(first_day_last_month, last_day_last_month))
    .join(establishment, "DocumentNumberCd", "left")
    .join(fi_registration, "DocumentNumberCd", "left")
    .join(fund_registration, "DocumentNumberCd", "left")
    .where((col("FiCvmCd").isNull()) & (col("FundRegistrationId").isNull()))
    .withColumn("DaysDifference", datediff(current_date(), col("ActivityStartDt")))
    .where(col("DaysDifference") <= lit(365))
    .select(
        "DocumentNumberCd",
        "ParticipantId",
        "ParticipantName",
        col("ActivityStartDt").alias("CompanyActivityStartDt"),
        col("CreatedAtDttm").alias("CercCreatedAt"),
        "DaysDifference"
    )
)
```

### participants-incorrectly-registered-as-fidc  
`cadastra` · **ACTIVE** · MONTHLY · threshold=0 · risco=R451 · área=TI Corporativa  

> Este teste possui o objetivo de identificar participantes ativos do sistema CERC registrados como FIDC que não apresentam correspondência nos registros oficiais de fundos mantidos pela CVM, configurando uma possível inconsistência cadastral.

```python
df_participant = (
    spark.table("trusted.payment_scheme_record.tb_participant")
    .where(col("StatusId") == "1")
)

df_fi_registration = (
    spark.table("trusted.cvm_public_data.tb_fi_registration")
    .select(
        col("FundDocumentCd").alias("DocumentNumberCd"),
        col("CvmCd").alias("FiCvmCd")
    )
)

df_fund_registration = (
    spark.table("trusted.cvm_public_data.tb_fund_registration")
    .select(
        col("FundDocumentCd").alias("DocumentNumberCd"),
        "FundRegistrationId"
    )
)

df_participant_nature_operation = (
    spark.table("trusted.payment_scheme_record.tb_participant_nature_operation")
    .where(col("StatusId") == 1)
    .select("ParticipantId", "NatureOperationId")
)

df_nature_operation = (
    spark.table("trusted.payment_scheme_record.tb_nature_operation")
    .select("NatureOperationId", "DescriptionDesc")
)

df_joined = (
    df_participant
    .join(df_fi_registration, on="DocumentNumberCd", how="left")
    .join(df_fund_registration, on="DocumentNumberCd", how="left")
    .join(df_participant_nature_operation, on="ParticipantId", how="left")
    .join(df_nature_operation, on="NatureOperationId", how="left")
)

df_incidents = (
    df_joined
    .where((col("FiCvmCd").isNull()) & (col("FundRegistrationId").isNull()))
    .where(col("DescriptionDesc") == lit("FIDC"))
    .select(
        "DocumentNumberCd",
        "ParticipantId",
        "ParticipantName",
        "NatureOperationId",
        "DescriptionDesc",
        "CreatedAtDttm",
        "FiCvmCd",
        "FundRegistrationId",
    )
)
```

### participants-joined-cerc-same-month-as-cnpj-creation  
`cadastra` · **ACTIVE** · MONTHLY · threshold=0 · risco=R313 · área=Riscos e Compliance  

> Este teste identifica participantes do sistema CERC - com exclusão de FIDCs - cujo CNPJ foi criado e que se tornaram participantes da CERC no mesmo mês, o que pode indicar risco de fraude, registros fictícios ou onboarding sem validação adequada.

```python
first_day_last_month = trunc(add_months(current_date(), -1), "month")
last_day_last_month = last_day(add_months(current_date(), -1))

establishment = (
    spark.table("trusted.irs_public_data.tb_establishment")
    .withColumnRenamed("DocumentCd", "DocumentNumberCd")
)

participants = (
    spark.table("trusted.payment_scheme_record.tb_participant")
    .where(col("StatusId") == lit("1"))
)

fi_registration = (
    spark.table("trusted.cvm_public_data.tb_fi_registration")
    .select(
        col("FundDocumentCd").alias("DocumentNumberCd"),
        col("CvmCd").alias("FiCvmCd")
    )
)

fund_registration = (
    spark.table("trusted.cvm_public_data.tb_fund_registration")
    .select(
        col("FundDocumentCd").alias("DocumentNumberCd"),
        "FundRegistrationId"
    )
)

df_incidents = (
    participants
    .where(col("CreatedAtDttm").between(first_day_last_month, last_day_last_month))
    .join(establishment, "DocumentNumberCd", "left")
    .join(fi_registration, "DocumentNumberCd", "left")
    .join(fund_registration, "DocumentNumberCd", "left")
    .where((col("FiCvmCd").isNull()) & (col("FundRegistrationId").isNull()))
    .withColumn("DaysDifference", datediff(col("CreatedAtDttm"), col("ActivityStartDt")))
    .where(col("DaysDifference") <= lit(30))
    .select(
        "DocumentNumberCd",
        "ParticipantId",
        "ParticipantName",
        "FiCvmCd",
        "FundRegistrationId",
        "ActivityStartDt",
        "CreatedAtDttm",
        "DaysDifference"
    )
)
```

### participants-with-duplicate-documents  
`cadastra` · **ACTIVE** · MONTHLY · threshold=0 · risco=R201 · área=TI Corporativa  

> Este teste identifica se há participantes no sistema CERC que compartilham o mesmo CNPJ, verificando a existência de registros duplicados entre diferentes participantes.

```python
df_incidents = (
    spark.table("trusted.payment_scheme_record.tb_participant")
    .where(col("StatusId") == lit("1"))
    .groupBy("DocumentNumberCd")
    .agg(countDistinct("ParticipantId").alias("CountParticipantId"))
    .where(col("CountParticipantId") > lit("1"))
)

display(df_incidents)
```

### participants-with-judicial-sanctions  
`cadastra` · **ACTIVE** · MONTHLY · threshold=0 · risco=R313 · área=Riscos e Compliance  

> Este teste identifica se os participantes do sistema CERC possuem sanções judiciais, incluindo registros no CNEP, CEPIM, CEIS ou em Acordos de Leniência.

```python
participant = (
    spark.table("trusted.payment_scheme_record.tb_participant")
    .where(col("StatusId") == lit("1"))
    .select(
        "ParticipantId",
        "DocumentNumberCd",
        "ParticipantName",
        "StatusId"
    )
)

ceis = (
    spark.table("trusted.judicial_sanctions_public_data.tb_ceis")
    .select(
        col("SanctionedCpfOrCnpjNum").alias("DocumentNumberCd"),
        concat_ws("-", col("SanctionCodeCd"), col("SanctionCategoryDesc")).alias("CEISIncidence")
    )
)

cepim = (
    spark.table("trusted.judicial_sanctions_public_data.tb_cepim")
    .select(
        col("EntityCnpjNum").alias("DocumentNumberCd"),
        concat_ws("-", col("AgreementNum"), col("GrantingAgencyDesc")).alias("CEPIMIncidence")
    )
)

cnep = (
    spark.table("trusted.judicial_sanctions_public_data.tb_cnep")
    .select(
        col("SanctionedCpfOrCnpjNum").alias("DocumentNumberCd"),
        concat_ws("-", col("SanctionCodeCd"), col("SanctionCategoryDesc")).alias("CNEPIncidence")
    )
)

leniency = (
    spark.table("trusted.judicial_sanctions_public_data.tb_leniency_agreements")
    .select(
        col("SanctionedCompanyCnpjNum").alias("DocumentNumberCd"),
        concat_ws("-", col("ProcessNum"), col("SanctioningOfficeDesc")).alias("LeniencyIncidence")
    )
)

df_incidents = (
    participant
    .join(ceis, "DocumentNumberCd", "left")
    .join(cepim, "DocumentNumberCd", "left")
    .join(cnep, "DocumentNumberCd", "left")
    .join(leniency, "DocumentNumberCd", "left")
    .where(
        (col("CEISIncidence").isNotNull()) |
        (col("CEPIMIncidence").isNotNull()) |
        (col("CNEPIncidence").isNotNull()) |
        (col("LeniencyIncidence").isNotNull())
    )
    .dropDuplicates(["DocumentNumberCd"])
)

df_incidents.display()
```

### participants-with-unmatched-document-number  
`cadastra` · **ACTIVE** · MONTHLY · threshold=0 · risco=R451 · área=TI Corporativa  

> Este teste identifica se há participantes ativos no sistema CERC cujo número de documento (CNPJ) não possui correspondência na base de estabelecimentos da Receita Federal, indicando registros com documentos inválidos ou não cadastrados.

```python
establishment = (
    spark.table("trusted.irs_public_data.tb_establishment")
    .withColumnRenamed("DocumentCd", "DocumentNumberCd")
)

participants = (
    spark.table("trusted.payment_scheme_record.tb_participant")
    .where(col("StatusId") == lit("1"))
)

df_incidents = (
    participants
    .join(establishment, "DocumentNumberCd", "left")
    .where(col("RootDocumentCd").isNull())
    .select(
        "DocumentNumberCd",
        "ParticipantId",
        "ParticipantName",
        "DocumentTypeId",
        "CreatedAtDttm",
        "CreatedById",
        "RootDocumentCd"
    )
)

df_incidents.display()
```

### participants-without-digital-onboarding  
`cadastra` · **ACTIVE** · MONTHLY · threshold=0 · risco=R393 · área=TI Corporativa  

> Este teste tem como objetivo identificar os participantes ativos do sistema CERC que não passaram pelo processo de onboarding digital.

```python
first_day_last_month = trunc(add_months(current_date(), -1), "month")
last_day_last_month = last_day(add_months(current_date(), -1))

establishment = (
    spark.table("trusted.irs_public_data.tb_establishment")
    .withColumnRenamed("DocumentCd", "DocumentNumberCd")
)

participants = (
    spark.table("trusted.payment_scheme_record.tb_participant")
    .where(col("StatusId") == lit("1"))
)

fi_registration = (
    spark.table("trusted.cvm_public_data.tb_fi_registration")
    .select(
        col("FundDocumentCd").alias("DocumentNumberCd"),
        "FundSocialName"
    )
)

fund_registration = (
    spark.table("trusted.cvm_public_data.tb_fund_registration")
    .select(
        col("FundDocumentCd").alias("DocumentNumberCd"),
        "FundRegistrationId"
    )
)

df_incidents = (
    participants
    .where(col("CreatedAtDttm").between(first_day_last_month, last_day_last_month))
    .join(establishment, "DocumentNumberCd", "left")
    .join(fi_registration, "DocumentNumberCd", "left")
    .join(fund_registration, "DocumentNumberCd", "left")
    .where((col("FundSocialName").isNull()) & (col("FundRegistrationId").isNull()))
    .withColumn("DaysDifference", datediff(col("CreatedAtDttm"), col("ActivityStartDt")))
    .where(col("DaysDifference") <= lit(30))
    .select(
        "DocumentNumberCd",
        "ParticipantId",
        "ParticipantName",
        "FundSocialName",
        "FundRegistrationId",
        "ActivityStartDt",
        "CreatedAtDttm",
        "DaysDifference"
    )
)
```

### participants-without-valid-cnpj  
`cadastra` · **ACTIVE** · MONTHLY · threshold=0 · risco=R313 · área=Riscos e Compliance  

> Este teste identifica se os participantes do sistema CERC possuem CNPJ com situação cadastral ativa na Receita Federal. CNPJs com situação diferente de 'ATIVA' são considerados inválidos.

```python
participant = (
    spark.table("trusted.payment_scheme_record.tb_participant")
    .where(col("StatusId") == lit("1"))
    .select(
        "ParticipantId",
        "DocumentNumberCd",
        "ParticipantName",
        "StatusId"
    )
)

establishment = (
    spark.table("trusted.irs_public_data.tb_establishment")
    .select(
        col("DocumentCd").alias("DocumentNumberCd"),
        "RegistrationStatusReasonCd",
        "RegistrationStatusDt"
    )
)

status_reason = (
    spark.table("trusted.irs_public_data.tb_registration_status_reason")
    .select(
        "RegistrationStatusReasonCd",
        "RegistrationStatusReasonDesc"
    )
)

nature_operation = (
    spark.table("trusted.payment_scheme_record.tb_nature_operation")
    .where(col("StatusCd") == lit("1"))
    .select(
        "NatureOperationId",
        "DescriptionDesc"
    )
)

participant_nature_operation = (
    spark.table("trusted.payment_scheme_record.tb_participant_nature_operation")
    .select(
        "ParticipantId",
        "NatureOperationId"
    )
    .join(nature_operation, "NatureOperationId", "left")
    .groupBy("ParticipantId")
    .agg(collect_set("DescriptionDesc").alias("OperationNatureDesc"))       
)

df_incidents = (
    participant
    .join(establishment, "DocumentNumberCd", "left")
    .join(status_reason, "RegistrationStatusReasonCd", "left")
    .join(participant_nature_operation, "ParticipantId", "left")
    .where(col("RegistrationStatusReasonCd") != lit("00"))  # ATIVA
    .select(
        "ParticipantId",
        "DocumentNumberCd",
        "ParticipantName",
        "StatusId",
        "RegistrationStatusDt",
        "OperationNatureDesc",
        "RegistrationStatusReasonCd",
        "RegistrationStatusReasonDesc"
    )
)
```

### participants-without-valid-email  
`cadastra` · **ACTIVE** · MONTHLY · threshold=50 · risco=R451 · área=TI Corporativa  

> Este teste identifica participantes ativos no sistema CERC cujo e-mail institucional contém a palavra “cerc” ou está ausente (nulo), indicando registros com informações de contato inválidas ou incompletas.

```python
df_incidents = (
    spark.table("trusted.payment_scheme_record.tb_participant")
    .where(col("StatusId") == lit("1"))
    .where(col("EmailName").contains("cerc") | col("EmailName").isNull())
    .select(
        "ParticipantId",
        "DocumentNumberCd",
        "ParticipantName",
        "EmailName",
        "CreatedAtDttm",
        "CreatedById"
    )
)

display(df_incidents)
```

### participants-without-valid-phone-number  
`cadastra` · **ACTIVE** · MONTHLY · threshold=50 · risco=R451 · área=TI Corporativa  

> Este teste identifica participantes do sistema CERC que não possuem um número de telefone válido, considerando como inválidos os casos em que o campo está nulo, vazio, possui menos de 8 dígitos ou contém sequências numéricas repetitivas (como “12345”, “11111”, etc.).

```python
from functools import reduce

invalid_phone = (
    col("Phone1Num").isNull()
    | (col("Phone1Num") == "")
    | (length("Phone1Num") < 8)
    | reduce(
        lambda a, b: a | b,
        [col("Phone1Num").contains(p) for p in (["12345"] + [str(i) * 5 for i in range(10)])],
    )
)

df_incidents = (
    spark.table("trusted.payment_scheme_record.tb_participant")
    .where((col("StatusId") == "1") & invalid_phone)
)

display(df_incidents)
```

### users-with-many-distinct-ips-per-day  
`cadastra` · **ACTIVE** · MONTHLY · threshold=0 · risco=R442 · área=TI Corporativa  

> Este teste identifica usuários do sistema CERC que acessam a plataforma a partir de muitos endereços IP distintos em um mesmo dia, o que pode indicar compartilhamento indevido de contas, uso de automações, comprometimento de credenciais ou outras atividades suspeitas.

```python
first_day_last_month = trunc(add_months(current_date(), -1), "month")
last_day_last_month  = last_day(add_months(current_date(), -1))

base = (
    spark.table("trusted.payment_scheme_record.tb_historic")
    .where(col("CreatedAtDttm").between(first_day_last_month, last_day_last_month))
    .withColumn("Date", to_date("CreatedAtDttm"))
    .withColumn("Action", trim(regexp_extract("DescriptionTxt", r"\[([^\]]+)\]", 1)))
    .withColumn(
        "UserId",
        nullif(
            trim(regexp_extract("DescriptionTxt", r"user/participant id:\s*([^\]]+)\]", 1)),
            lit("")
        )
    )
    .join(
        spark.table("trusted.payment_scheme_record.tb_user")
            .select("UserId", "UserName", "EmailAddressTxt", "StatusId", "DocumentNumberNum"),
        "UserId",
        "left"
    )
    .dropDuplicates()
)

df_incidents = (
    base.select("EmailAddressTxt", "UserId", "CreatedByIpCd", "Date")
    .na.drop(subset=["EmailAddressTxt", "CreatedByIpCd", "Date"])
    .groupBy("EmailAddressTxt", "UserId", "Date")
    .agg(
        countDistinct("CreatedByIpCd").alias("DistinctIPCount"),
        collect_set("CreatedByIpCd").alias("DistinctIPs"),
    )
    .where(col("DistinctIPCount") > lit(3))
)
```

### ccb-dc-data-quality-relevant-timelines-after-tech-updates-10-dec-2025  
`cedulas` · **ACTIVE** · MONTHLY · threshold=0 · risco=R1472 · área=Recebíveis Comerciais​  

> Diagnosticar reincidência da inconsistência de registros de CCBs e DCs. Esse teste considera o cenário de CCBs e DCs com data de vencimento antes da data de registro para novos registros, após correções aplicadas em 10-Dez-2025 pelos times de TI da CERC para corrigir de forma definitiva esse problema.

```python
#Lógica do teste - CENÁRIO NOVOS REGISTROS (após 10-Dez-2025)

from pyspark.sql.functions import col
import traceback

df_incidents = spark.sql("""

WITH parsed AS (
  SELECT
    AssetId,
    TypeAssetId,
    CASE TypeAssetId
      WHEN 1  THEN 'CCB'  WHEN 2  THEN 'CPR'  WHEN 3  THEN 'CDCA' WHEN 4  THEN 'ACC'
      WHEN 5  THEN 'CCCM' WHEN 6  THEN 'CCE'  WHEN 7  THEN 'CCES' WHEN 8  THEN 'CCIN'
      WHEN 9  THEN 'CDAWA' WHEN 10 THEN 'CDIV' WHEN 11 THEN 'CMER' WHEN 12 THEN 'CMUT'
      WHEN 13 THEN 'CRH'  WHEN 14 THEN 'CRP'  WHEN 15 THEN 'CRPH' WHEN 16 THEN 'DEB'
      WHEN 17 THEN 'DUPR' WHEN 18 THEN 'NC'   WHEN 19 THEN 'NCCM' WHEN 20 THEN 'NCE'
      WHEN 21 THEN 'NCIN' WHEN 22 THEN 'NCR'  WHEN 23 THEN 'CPR_VERDE' WHEN 24 THEN 'NOTA_PROMISSORIA'
      WHEN 25 THEN 'DUPLICATAS' WHEN 26 THEN 'DUPLICATA_MERCANTIL_CERC' WHEN 27 THEN 'DUPLICATA_SERVICO_CERC'
      WHEN 28 THEN 'CHEQUE_CERC' WHEN 29 THEN 'CONTRATO_CERC' WHEN 30 THEN 'NOTA_PROMISSORIA_CERC'
      WHEN 31 THEN 'CONTRATO_DIREITO_CREDITORIO_CERC' WHEN 32 THEN 'NOTA_FISCAL_ELETRONICA_CERC'
      WHEN 33 THEN 'CIR' WHEN 34 THEN 'CRI' WHEN 35 THEN 'CRA' WHEN 36 THEN 'CR'
      WHEN 37 THEN 'LCI' WHEN 38 THEN 'LCA' WHEN 39 THEN 'DC'  WHEN 40 THEN 'CCI'
      WHEN 41 THEN 'CCCB' WHEN 999 THEN 'OUTROS'
      ELSE CAST(TypeAssetId AS STRING)
    END AS TypeAssetName,
    StatusAssetId,
    RegistryDttm,
    DueDt,

    COALESCE(
      DATE(RegistryDttm),
      to_date(cast(RegistryDttm AS string), 'yyyy-MM-dd'),
      to_date(cast(RegistryDttm AS string), 'dd/MM/yyyy'),
      to_date(cast(RegistryDttm AS string), 'dd-MM-yyyy')
    ) AS RegistryDate_parsed,

    COALESCE(
      DATE(DueDt),
      to_date(cast(DueDt AS string), 'yyyy-MM-dd'),
      to_date(cast(DueDt AS string), 'dd/MM/yyyy'),
      to_date(cast(DueDt AS string), 'dd-MM-yyyy')
    ) AS DueDate_parsed

  FROM trusted.banking_credit_notes.tb_asset
  WHERE TypeAssetId IN (1, 39) -- CCB, DC
)

SELECT
  AssetId,
  TypeAssetId,
  TypeAssetName,
  StatusAssetId,
  RegistryDttm       AS Registry_orig,
  DueDt              AS Due_orig,
  RegistryDate_parsed,
  DueDate_parsed,
  datediff(DueDate_parsed, RegistryDate_parsed) AS Days_Due_minus_Registry,
  'DEPOIS 10-Dez-2025' AS moment

FROM parsed
WHERE
  RegistryDate_parsed IS NOT NULL
  AND DueDate_parsed IS NOT NULL
  AND datediff(DueDate_parsed, RegistryDate_parsed) < 0 -- Due_date antes de Registry_date (problema)
  AND RegistryDate_parsed > to_date('2025-12-10')

ORDER BY Days_Due_minus_Registry ASC, RegistryDate_parsed DESC

""")
```

### ccb-dc-data-quality-relevant-timelines-before-tech-updates-10-dec-2025  
`cedulas` · **ACTIVE** · MONTHLY · threshold=0 · risco=R1472 · área=Projetos Não-Regulatórios  

> Diagnosticar inconsistência de registros de CPRs. Esse teste considera o cenário de CPRs com data de vencimento antes da data de registro para o estoque dos registros, antes das correções aplicadas em 10-Dez-2025 pelos times de TI da CERC para corrigir de forma definitiva esse problema. Impacto regulatório dado acordo realizado pela CERC com o Banco Central do Brasil em 23-Out-2025 por meio de Termo de Compromisso para estar em conformidade com a Instrução Normativa n° 520 de 6-Set-2024.

```python
#Lógica do teste - CENÁRIO ESTOQUE (até 10-Dez-2025)

from pyspark.sql.functions import col
import traceback

df_incidents = spark.sql("""

WITH parsed AS (
  SELECT
    AssetId,
    TypeAssetId,
    CASE TypeAssetId
      WHEN 1  THEN 'CCB'  WHEN 2  THEN 'CPR'  WHEN 3  THEN 'CDCA' WHEN 4  THEN 'ACC'
      WHEN 5  THEN 'CCCM' WHEN 6  THEN 'CCE'  WHEN 7  THEN 'CCES' WHEN 8  THEN 'CCIN'
      WHEN 9  THEN 'CDAWA' WHEN 10 THEN 'CDIV' WHEN 11 THEN 'CMER' WHEN 12 THEN 'CMUT'
      WHEN 13 THEN 'CRH'  WHEN 14 THEN 'CRP'  WHEN 15 THEN 'CRPH' WHEN 16 THEN 'DEB'
      WHEN 17 THEN 'DUPR' WHEN 18 THEN 'NC'   WHEN 19 THEN 'NCCM' WHEN 20 THEN 'NCE'
      WHEN 21 THEN 'NCIN' WHEN 22 THEN 'NCR'  WHEN 23 THEN 'CPR_VERDE' WHEN 24 THEN 'NOTA_PROMISSORIA'
      WHEN 25 THEN 'DUPLICATAS' WHEN 26 THEN 'DUPLICATA_MERCANTIL_CERC' WHEN 27 THEN 'DUPLICATA_SERVICO_CERC'
      WHEN 28 THEN 'CHEQUE_CERC' WHEN 29 THEN 'CONTRATO_CERC' WHEN 30 THEN 'NOTA_PROMISSORIA_CERC'
      WHEN 31 THEN 'CONTRATO_DIREITO_CREDITORIO_CERC' WHEN 32 THEN 'NOTA_FISCAL_ELETRONICA_CERC'
      WHEN 33 THEN 'CIR' WHEN 34 THEN 'CRI' WHEN 35 THEN 'CRA' WHEN 36 THEN 'CR'
      WHEN 37 THEN 'LCI' WHEN 38 THEN 'LCA' WHEN 39 THEN 'DC'  WHEN 40 THEN 'CCI'
      WHEN 41 THEN 'CCCB' WHEN 999 THEN 'OUTROS'
      ELSE CAST(TypeAssetId AS STRING)
    END AS TypeAssetName,
    StatusAssetId,
    RegistryDttm,
    DueDt,

    COALESCE(
      DATE(RegistryDttm),
      to_date(cast(RegistryDttm AS string), 'yyyy-MM-dd'),
      to_date(cast(RegistryDttm AS string), 'dd/MM/yyyy'),
      to_date(cast(RegistryDttm AS string), 'dd-MM-yyyy')
    ) AS RegistryDate_parsed,

    COALESCE(
      DATE(DueDt),
      to_date(cast(DueDt AS string), 'yyyy-MM-dd'),
      to_date(cast(DueDt AS string), 'dd/MM/yyyy'),
      to_date(cast(DueDt AS string), 'dd-MM-yyyy')
    ) AS DueDate_parsed

  FROM trusted.banking_credit_notes.tb_asset
  WHERE TypeAssetId IN (1, 39) -- CCB, DC
)

SELECT
  AssetId,
  TypeAssetId,
  TypeAssetName,
  StatusAssetId,
  RegistryDttm       AS Registry_orig,
  DueDt              AS Due_orig,
  RegistryDate_parsed,
  DueDate_parsed,
  datediff(DueDate_parsed, RegistryDate_parsed) AS Days_Due_minus_Registry,
  'ATE_10-Dez-2025' AS moment

FROM parsed
WHERE
  RegistryDate_parsed IS NOT NULL
  AND DueDate_parsed IS NOT NULL
  AND datediff(DueDate_parsed, RegistryDate_parsed) < 0 -- Due_date antes de Registry_date (problema)
  AND RegistryDate_parsed <= DATE('2025-12-10')
ORDER BY Days_Due_minus_Registry ASC, RegistryDate_parsed DESC

""")
```

### cpr-data-quality-relevant-timelines-after-tech-updates-10-dec-2025  
`cedulas` · **ACTIVE** · MONTHLY · threshold=0 · risco=R1470 · área=Engenharia de Produtos  

> Diagnosticar reincidência da inconsistência de registros de CPRs. Esse teste considera o cenário de CPRs com data de vencimento antes da data de registro para novos registros, após correções aplicadas em 10-Dez-2025 pelos times de TI da CERC para corrigir de forma definitiva esse problema. Impacto regulatório dado acordo realizado pela CERC com o Banco Central do Brasil em 23-Out-2025 por meio de Termo de Compromisso para estar em conformidade com a Instrução Normativa n° 520 de 6-Set-2024.

```python
#Lógica do teste - CENÁRIO NOVOS REGISTROS (após 10-Dez-2025)

from pyspark.sql.functions import col
import traceback

df_incidents = spark.sql("""

WITH parsed AS (
  SELECT
    AssetId,
    TypeAssetId,
    CASE TypeAssetId
      WHEN 1  THEN 'CCB'  WHEN 2  THEN 'CPR'  WHEN 3  THEN 'CDCA' WHEN 4  THEN 'ACC'
      WHEN 5  THEN 'CCCM' WHEN 6  THEN 'CCE'  WHEN 7  THEN 'CCES' WHEN 8  THEN 'CCIN'
      WHEN 9  THEN 'CDAWA' WHEN 10 THEN 'CDIV' WHEN 11 THEN 'CMER' WHEN 12 THEN 'CMUT'
      WHEN 13 THEN 'CRH'  WHEN 14 THEN 'CRP'  WHEN 15 THEN 'CRPH' WHEN 16 THEN 'DEB'
      WHEN 17 THEN 'DUPR' WHEN 18 THEN 'NC'   WHEN 19 THEN 'NCCM' WHEN 20 THEN 'NCE'
      WHEN 21 THEN 'NCIN' WHEN 22 THEN 'NCR'  WHEN 23 THEN 'CPR_VERDE' WHEN 24 THEN 'NOTA_PROMISSORIA'
      WHEN 25 THEN 'DUPLICATAS' WHEN 26 THEN 'DUPLICATA_MERCANTIL_CERC' WHEN 27 THEN 'DUPLICATA_SERVICO_CERC'
      WHEN 28 THEN 'CHEQUE_CERC' WHEN 29 THEN 'CONTRATO_CERC' WHEN 30 THEN 'NOTA_PROMISSORIA_CERC'
      WHEN 31 THEN 'CONTRATO_DIREITO_CREDITORIO_CERC' WHEN 32 THEN 'NOTA_FISCAL_ELETRONICA_CERC'
      WHEN 33 THEN 'CIR' WHEN 34 THEN 'CRI' WHEN 35 THEN 'CRA' WHEN 36 THEN 'CR'
      WHEN 37 THEN 'LCI' WHEN 38 THEN 'LCA' WHEN 39 THEN 'DC'  WHEN 40 THEN 'CCI'
      WHEN 41 THEN 'CCCB' WHEN 999 THEN 'OUTROS'
      ELSE CAST(TypeAssetId AS STRING)
    END AS TypeAssetName,
    StatusAssetId,
    RegistryDttm,
    DueDt,

    COALESCE(
      DATE(RegistryDttm),
      to_date(cast(RegistryDttm AS string), 'yyyy-MM-dd'),
      to_date(cast(RegistryDttm AS string), 'dd/MM/yyyy'),
      to_date(cast(RegistryDttm AS string), 'dd-MM-yyyy')
    ) AS RegistryDate_parsed,

    COALESCE(
      DATE(DueDt),
      to_date(cast(DueDt AS string), 'yyyy-MM-dd'),
      to_date(cast(DueDt AS string), 'dd/MM/yyyy'),
      to_date(cast(DueDt AS string), 'dd-MM-yyyy')
    ) AS DueDate_parsed

  FROM trusted.banking_credit_notes.tb_asset
  WHERE TypeAssetId = 2 -- CPRs
)

SELECT
  AssetId,
  TypeAssetId,
  TypeAssetName,
  StatusAssetId,
  RegistryDttm       AS Registry_orig,
  DueDt              AS Due_orig,
  RegistryDate_parsed,
  DueDate_parsed,
  datediff(DueDate_parsed, RegistryDate_parsed) AS Days_Due_minus_Registry,
  'DEPOIS 10-Dez-2025' AS moment

FROM parsed
WHERE
  RegistryDate_parsed IS NOT NULL
  AND DueDate_parsed IS NOT NULL
  AND datediff(DueDate_parsed, RegistryDate_parsed) < 0 -- Due_date antes de Registry_date (problema)
  AND RegistryDate_parsed > to_date('2025-12-10')

ORDER BY Days_Due_minus_Registry ASC, RegistryDate_parsed DESC

""")
```

### cpr-data-quality-relevant-timelines-before-tech-updates-10-dec-2025  
`cedulas` · **PAUSED** · MONTHLY · threshold=0 · risco=R1473 · área=Monitoramento de Mercado  

> Diagnosticar inconsistência de registros de CPRs. Esse teste considera o cenário de CPRs com data de vencimento antes da data de registro para o estoque dos registros, antes das correções aplicadas em 10-Dez-2025 pelos times de TI da CERC para corrigir de forma definitiva esse problema. Impacto regulatório dado acordo realizado pela CERC com o Banco Central do Brasil em 23-Out-2025 por meio de Termo de Compromisso para estar em conformidade com a Instrução Normativa n° 520 de 6-Set-2024.

```python
#Lógica do teste - CENÁRIO ESTOQUE (até 10-Dez-2025)

from pyspark.sql.functions import col
import traceback

df_incidents = spark.sql("""

WITH parsed AS (
  SELECT
    AssetId,
    TypeAssetId,
    CASE TypeAssetId
      WHEN 1  THEN 'CCB'  WHEN 2  THEN 'CPR'  WHEN 3  THEN 'CDCA' WHEN 4  THEN 'ACC'
      WHEN 5  THEN 'CCCM' WHEN 6  THEN 'CCE'  WHEN 7  THEN 'CCES' WHEN 8  THEN 'CCIN'
      WHEN 9  THEN 'CDAWA' WHEN 10 THEN 'CDIV' WHEN 11 THEN 'CMER' WHEN 12 THEN 'CMUT'
      WHEN 13 THEN 'CRH'  WHEN 14 THEN 'CRP'  WHEN 15 THEN 'CRPH' WHEN 16 THEN 'DEB'
      WHEN 17 THEN 'DUPR' WHEN 18 THEN 'NC'   WHEN 19 THEN 'NCCM' WHEN 20 THEN 'NCE'
      WHEN 21 THEN 'NCIN' WHEN 22 THEN 'NCR'  WHEN 23 THEN 'CPR_VERDE' WHEN 24 THEN 'NOTA_PROMISSORIA'
      WHEN 25 THEN 'DUPLICATAS' WHEN 26 THEN 'DUPLICATA_MERCANTIL_CERC' WHEN 27 THEN 'DUPLICATA_SERVICO_CERC'
      WHEN 28 THEN 'CHEQUE_CERC' WHEN 29 THEN 'CONTRATO_CERC' WHEN 30 THEN 'NOTA_PROMISSORIA_CERC'
      WHEN 31 THEN 'CONTRATO_DIREITO_CREDITORIO_CERC' WHEN 32 THEN 'NOTA_FISCAL_ELETRONICA_CERC'
      WHEN 33 THEN 'CIR' WHEN 34 THEN 'CRI' WHEN 35 THEN 'CRA' WHEN 36 THEN 'CR'
      WHEN 37 THEN 'LCI' WHEN 38 THEN 'LCA' WHEN 39 THEN 'DC'  WHEN 40 THEN 'CCI'
      WHEN 41 THEN 'CCCB' WHEN 999 THEN 'OUTROS'
      ELSE CAST(TypeAssetId AS STRING)
    END AS TypeAssetName,
    StatusAssetId,
    RegistryDttm,
    DueDt,

    COALESCE(
      DATE(RegistryDttm),
      to_date(cast(RegistryDttm AS string), 'yyyy-MM-dd'),
      to_date(cast(RegistryDttm AS string), 'dd/MM/yyyy'),
      to_date(cast(RegistryDttm AS string), 'dd-MM-yyyy')
    ) AS RegistryDate_parsed,

    COALESCE(
      DATE(DueDt),
      to_date(cast(DueDt AS string), 'yyyy-MM-dd'),
      to_date(cast(DueDt AS string), 'dd/MM/yyyy'),
      to_date(cast(DueDt AS string), 'dd-MM-yyyy')
    ) AS DueDate_parsed

  FROM trusted.banking_credit_notes.tb_asset
  WHERE TypeAssetId = 2 -- CPRs
)

SELECT
  AssetId,
  TypeAssetId,
  TypeAssetName,
  StatusAssetId,
  RegistryDttm       AS Registry_orig,
  DueDt              AS Due_orig,
  RegistryDate_parsed,
  DueDate_parsed,
  datediff(DueDate_parsed, RegistryDate_parsed) AS Days_Due_minus_Registry,
  'ATE_10-Dez-2025' AS moment

FROM parsed
WHERE
  RegistryDate_parsed IS NOT NULL
  AND DueDate_parsed IS NOT NULL
  AND datediff(DueDate_parsed, RegistryDate_parsed) < 0 -- Due_date antes de Registry_date (problema)
  AND RegistryDate_parsed <= DATE('2025-12-10')
ORDER BY Days_Due_minus_Registry ASC, RegistryDate_parsed DESC

""")
```

### index-below-regulatory-requirement  
`cerc-system-availability` · **PAUSED** · DAILY · threshold=0 · risco=R294 · área=Infraestrutura  

> O objetivo deste teste é verificar se, do ponto de vista do cliente, a disponibilidade do Sistema CERC pelos canais HTTPS (Portal e APIs) de cada sistema e endpoint se manteve acima do índice mínimo de 99,8%, identificando dias em que o indicador de disponibilidade caiu abaixo desse patamar.

```python
df_incidents = (
    spark.table("trusted.infracorp.tb_availability_http")
    .where(col("EventDt") == date_sub(current_date(), 1))
    .groupBy(
        col("EventDt"),
        col("ProjectId"),
        col("EndpointTxt")
    )
    .agg(
        sum(col("RequestCountQty")).alias("total_requests"),
        sum(
            when(col("HttpStatusCd") >= 500, col("RequestCountQty")).otherwise(0)
        ).alias("failed_requests")
    )
    .withColumn(
        "availability_pct",
        (lit(1) - (col("failed_requests") / col("total_requests"))) * lit(100)
    )
    .where(col("availability_pct") < 99.8)
)
```

### claude-token-audit-log  
`claude-audit-log` · **ACTIVE** · DAILY · threshold=0 · risco=R1474 · área=Arquitetura  

> Detecta criacao/delecao de token e extracao explicita de dados via eventos de exportacao no Activity Feed da Compliance API (ultimos 3 dias).

```python
# Exemplo: Lógica do teste
import json
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from pyspark.sql.types import StringType, StructField, StructType

API_KEY = dbutils.secrets.get(scope="compliance-grc", key="claude-audit-api-key")
API_BASE = "https://api.anthropic.com/v1/compliance"
API_VERSION = "2026-03-29"

LOOKBACK_DAYS = 3
ORG_PAGE_LIMIT = 1000
ACTIVITY_PAGE_LIMIT = 1000
MAX_ORG_PAGES = 100
MAX_ACTIVITY_PAGES_PER_ORG = 200
MAX_RETRIES = 4

TOKEN_CREATED_OR_DELETED_TYPES = {
    "api_key_created",
    "api_key_deleted",
    "admin_api_key_created",
    "admin_api_key_deleted",
    "platform_api_key_created",
    "platform_api_key_deleted",
    "scoped_api_key_deleted",
}

EXPLICIT_EXPORT_TYPES = {
    "org_data_export_started",
    "org_data_export_completed",
    "org_members_exported",
    "student_data_exported",
}

end_at = datetime.now(timezone.utc).replace(microsecond=0)
start_at = end_at - timedelta(days=LOOKBACK_DAYS)
created_at_gte = start_at.isoformat().replace("+00:00", "Z")
created_at_lte = end_at.isoformat().replace("+00:00", "Z")

session = requests.Session()
session.headers.update(
    {
        "x-api-key": API_KEY,
        "anthropic-version": API_VERSION,
        "user-agent": "ContinuousAuditTokenExportTest/1.0",
    }
)

def compliance_get(path: str, params: list[tuple[str, str]] | None = None) -> dict[str, Any]:
    url = f"{API_BASE.rstrip('/')}/{path.lstrip('/')}"
    for attempt in range(MAX_RETRIES + 1):
        response = session.get(url, params=params, timeout=60)
        if response.status_code == 200:
            return response.json()

        if response.status_code in {408, 409, 425, 429, 500, 502, 503, 504} and attempt < MAX_RETRIES:
            time.sleep(min(0.5 * (2**attempt), 8.0))
            continue

        raise RuntimeError(f"Compliance API error ({response.status_code}) in {path}: {response.text[:400]}")

    raise RuntimeError(f"Failed after retries: {path}")

def list_all_organizations() -> list[dict[str, Any]]:
    organizations: list[dict[str, Any]] = []
    params: list[tuple[str, str]] = [("limit", str(ORG_PAGE_LIMIT))]

    for _ in range(MAX_ORG_PAGES):
        payload = compliance_get("/organizations", params=params)
        data = payload.get("data", [])
        if isinstance(data, list):
            organizations.extend(item for item in data if isinstance(item, dict))

        has_more = bool(payload.get("has_more"))
        next_page = payload.get("next_page")
        last_id = payload.get("last_id")

        if has_more and next_page:
            params = [(k, v) for k, v in params if k != "page"]
            params.append(("page", str(next_page)))
            continue
        if has_more and last_id:
            params = [(k, v) for k, v in params if k not in {"after_id", "before_id"}]
            params.append(("after_id", str(last_id)))
            continue
        break

    dedup: dict[str, dict[str, Any]] = {}
    for org in organizations:
        org_uuid = org.get("uuid") or org.get("id")
        if org_uuid:
            dedup[str(org_uuid)] = org
    return list(dedup.values())

def list_org_activities(org_uuid: str) -> list[dict[str, Any]]:
    params: list[tuple[str, str]] = [
        ("organization_ids[]", org_uuid),
        ("created_at.gte", created_at_gte),
        ("created_at.lte", created_at_lte),
        ("limit", str(ACTIVITY_PAGE_LIMIT)),
    ]

    events: list[dict[str, Any]] = []
    current = list(params)
    for _ in range(MAX_ACTIVITY_PAGES_PER_ORG):
        payload = compliance_get("/activities", params=current)
        data = payload.get("data", [])
        if isinstance(data, list):
            events.extend(item for item in data if isinstance(item, dict))

        has_more = bool(payload.get("has_more"))
        last_id = payload.get("last_id")
        if not has_more or not last_id:
            break

        current = [(k, v) for k, v in current if k not in {"after_id", "before_id"}]
        current.append(("after_id", str(last_id)))

    return events

def actor_identifier(event: dict[str, Any]) -> str:
    actor = event.get("actor") if isinstance(event.get("actor"), dict) else {}
    return (
        str(actor.get("email_address") or "")
        or str(actor.get("user_id") or "")
        or str(actor.get("api_key_id") or "")
        or str(actor.get("id") or "")
        or "unknown"
    )

def token_reference(event: dict[str, Any]) -> str:
    return str(event.get("api_key_id") or event.get("admin_api_key_id") or event.get("token_id") or "")

def build_incident(rule_id: str, event: dict[str, Any], evidence: str) -> dict[str, Any]:
    return {
        "incident_id": f"{rule_id}::{event.get('id')}",
        "rule_id": rule_id,
        "event_created_at": str(event.get("created_at") or ""),
        "activity_id": str(event.get("id") or ""),
        "activity_type": str(event.get("type") or ""),
        "organization_uuid": str(event.get("organization_uuid") or event.get("organization_id") or ""),
        "actor_identifier": actor_identifier(event),
        "token_reference": token_reference(event),
        "evidence": evidence,
        "raw_event": json.dumps(event, ensure_ascii=False, default=str),
    }

organizations = list_all_organizations()
org_ids = [str(org.get("uuid") or org.get("id")) for org in organizations if org.get("uuid") or org.get("id")]

all_events: list[dict[str, Any]] = []
for org_id in org_ids:
    all_events.extend(list_org_activities(org_id))

incidents: list[dict[str, Any]] = []
for event in all_events:
    event_type = str(event.get("type") or "").lower()

    if event_type in TOKEN_CREATED_OR_DELETED_TYPES:
        incidents.append(
            build_incident(
                rule_id="token_criado_ou_deletado",
                event=event,
                evidence="Evento de criacao/delecao de token/chave.",
            )
        )
        continue

    if event_type in EXPLICIT_EXPORT_TYPES:
        incidents.append(
            build_incident(
                rule_id="extracao_dados_por_exportacao",
                event=event,
                evidence="Evento explicito de exportacao de dados.",
            )
        )

dedup: dict[str, dict[str, Any]] = {}
for incident in incidents:
    dedup[str(incident["incident_id"])] = incident
incidents = list(dedup.values())

incident_schema = StructType(
    [
        StructField("incident_id", StringType(), True),
        StructField("rule_id", StringType(), True),
        StructField("event_created_at", StringType(), True),
        StructField("activity_id", StringType(), True),
        StructField("activity_type", StringType(), True),
        StructField("organization_uuid", StringType(), True),
        StructField("actor_identifier", StringType(), True),
        StructField("token_reference", StringType(), True),
        StructField("evidence", StringType(), True),
        StructField("raw_event", StringType(), True),
    ]
)

df_incidents = spark.createDataFrame(incidents, schema=incident_schema) if incidents else spark.createDataFrame([], schema=incident_schema)
```

### employees-with-other-active-companies  
`compliance` · **ACTIVE** · MONTHLY · threshold=0 · risco=R358 · área=Riscos e Compliance  

> Esse teste possui o objetivo de identificar os funcionários da CERC que também constam como sócios em empresas ativas no sistema do Publica, cruzando informações do cadastro interno de colaboradores com os registros públicos de empresas e sócios. O teste permite detectar possíveis conflitos de interesse ou sobreposição de funções entre a atuação como empregado e participação societária em empresas externas.

```python
exceptions = [
    ("MARIO MELLO FREIRE NETO", "BANCO J. SAFRA S.A"),
    ("FATIMA CAROLINA GILBERTO RIOS PEREIRA", "ANTECIPA S/A"),
    ("LEONARDO BORGES DA SILVA MARTINS", "AKKWA SERVIÇOS FINANCEIROS E TECNOLÓGICOS LTDA"),
    ("PAULO IONESCU", "ANTECIPA S/A"),
    ("MARIO MELLO FREIRE NETO", "BANCO SAFRA"),
]

exceptions_upper = [(name.upper(), company.upper()) for name, company in exceptions]

df_employees = (
    spark.table("compliance.cerc_data.tb_hr_employees")
    .select(
        "CorporateEmail",
        upper(col("EmployeeName")).alias("AssociateName"),
        "DepartmentName",
        "ManagementClassName",
        "HireDate",
    )
)

df_qsa = (
    spark.table("trusted.irs_public_data.tb_associate")
    .select(
        upper(col("AssociateName")).alias("AssociateName"),
        "AssociateQualificationCd",
        "AssociateDocumentCd",
        "RootDocumentCd",
    )
)

df_company = (
    spark.table("trusted.irs_public_data.tb_company")
    .select("RootDocumentCd", "CompanyName")
)

df_establishment = (
    spark.table("trusted.irs_public_data.tb_establishment")
    .select("RootDocumentCd", "DocumentCd")
)

df_participants = (
    spark.table("trusted.payment_scheme_record.tb_participant")
    .where(col("StatusId") == lit("1"))
    .select(
        "ParticipantId",
        col("DocumentNumberCd").alias("DocumentCd"),
        upper(col("ParticipantName")).alias("ParticipantName"),
        "CreatedAtDttm",
        "CreatedById",
    )
)

df_joined = (
    df_employees
    .join(df_qsa, on="AssociateName", how="left")
    .join(df_company, on="RootDocumentCd", how="left")
    .join(df_establishment, on="RootDocumentCd", how="left")
    .join(df_participants, on="DocumentCd", how="left")
    .where(col("ParticipantId").isNotNull())
    .where(col("DocumentCd") != lit("23399607000191")) 
)

exception_conditions = [
    (col("AssociateName") == lit(name)) & (col("ParticipantName") == lit(company))
    for name, company in exceptions_upper
]

df_incidents = df_joined.where(~(reduce(lambda a, b: a | b, exception_conditions, lit(False))))
```

### mandatory-trainings  
`compliance` · **PAUSED** · DAILY · threshold=0 · risco=R347 · área=CSC  

> Esse teste possui o objetivo de identificar os colaboradores que não realizaram os treinamentos de conformidade obrigatórios: Gestão de Riscos na CERC Guia Basico para Colaboradores e Conformidade e Ética: Guia Básico para Colaboradores.

```python
employees = (
    spark.table("sandbox.grc.tb_external_hr_employees")
    .withColumn("Email", lower(col("CorporateEmail")))
    .where(col("AdmissionDate") >= lit("2024-12-24"))
    .select("Email", "Name", "AdmissionDate", "DepartmentName")
)

knowbe4 = spark.table("sandbox.grc.tb_external_knowbe4_compliance_training")

df_incidents = (
employees
.join(knowbe4, "Email", "left")
.withColumn("DateDifference", datediff(current_timestamp(), col("AdmissionDate")))
.where((col("DateDifference") >= lit(30)) & (col("CompletionDate").isNull()))
.select(
    "Email",
    "Name",
    "AdmissionDate", 
    "DepartmentName",
    "ModuleName",
    "StartDate",
    "CompletionDate",
    "Status",
    "DateDifference"
)
)

df_incidents.display()
```

### access-control-list-changes  
`databricks-config` · **ACTIVE** · DAILY · threshold=0 · risco=R1463 · área=Engenharia e Tecnologia KYP  

> Este teste possui o objetivo de realizar uma verificação de todas as alterações feitas na Access Control List (ACL) no dia anterior - Representa qualquer modificação na política que define quem pode criar, listar ou revogar tokens de API dentro do Databricks. Ele identifica quais usuários executaram mudanças, quantas modificações foram realizadas e quais recursos foram impactados.

```python
df_incidents = (
    spark.table("system.access.audit")
    .where(
        (col("action_name") == lit("changeDbTokenAcl")) &
        (col("event_date") == date_sub(current_date(), 1))
    )
    .groupBy("user_identity.email")
    .agg(
        count("event_id").alias("count"),
        collect_set("account_id").alias("account_id"),
        collect_set("request_id").alias("request_id"),
        collect_set("event_id").alias("event_id"),
        collect_set("event_time").alias("event_time"),
        collect_list("request_params").alias("request_params"),
        collect_list("response").alias("response")
    )
)
```

### access-control-list-disable  
`databricks-config` · **ACTIVE** · DAILY · threshold=0 · risco=R1463 · área=Engenharia e Tecnologia KYP  

> Este teste possui o objetivo de verificar quando a Access Control List (ACL) foi desativada no Databricks. A ação disableTableAcls representa a remoção temporária ou permanente das regras de controle de acesso que definem quem pode criar, listar ou revogar tokens de API em uma tabela ou recurso específico.

```python
df_incidents = (
    spark.table("system.access.audit")
    .where(
        (col("action_name") == lit("disableTableAcls")) &
        (col("event_date") == date_sub(current_date(), 1))
    )
)
```

### download-large-results  
`databricks-config` · **ACTIVE** · DAILY · threshold=0 · risco=R271 · área=Engenharia e Tecnologia KYP  

> Este teste identifica usuários que realizaram o download dos resultados de grandes queries em ambiente de produção dentro do workspace da CERC, o que pode indicar risco de vazamento de dados, uso indevido de informações sensíveis ou descumprimento de políticas de segurança.

```python
from pyspark.sql.functions import *
import traceback


base = spark.table("system.access.audit").alias("a")

download_events = (
    base
    .filter(col("a.action_name") == "downloadLargeResults")
    .select(
        col("a.event_id").cast("string").alias("event_id"),
        col("a.request_id").cast("string").alias("request_id"),
        col("a.session_id").cast("string").alias("session_id"),
        col("a.event_time").alias("event_time"),
        col("a.user_identity").alias("download_user_identity"),
        col("a.service_name").cast("string").alias("service_name"),
        col("a.action_name").cast("string").alias("action_name"),
        col("a.request_params")["notebookId"].cast("string").alias("notebook_id"),
        col("a.request_params")["commandId"].cast("string").alias("command_id")
    )
)

qualified_users = (
    download_events
    .withColumn("event_day", to_date(col("event_time")))
    .filter(col("event_day") == date_sub(current_date(), 1))
    .groupBy("download_user_identity", "event_day")
    .agg(count("*").alias("download_count"))
    .filter(col("download_count") >= 5)
)

download_events_filtered = (
    download_events.alias("d")
    .join(
        qualified_users.alias("q"),
        on=[
            col("d.download_user_identity") == col("q.download_user_identity"),
            to_date(col("d.event_time")) == col("q.event_day")
        ],
        how="inner"
    )
    .select(
        col("d.event_id"),
        col("d.request_id"),
        col("d.session_id"),
        col("d.event_time"),
        col("d.download_user_identity"),
        col("d.notebook_id"),
        col("d.command_id")
    )
)

command_events = (
    base
    .filter(
        (col("a.service_name") == "notebook") &
        (col("a.action_name").isin("runCommand", "submitCommand")) &
        col("a.request_params")["commandText"].isNotNull()
    )
    .select(
        col("a.request_params")["notebookId"].cast("string").alias("notebook_id"),
        col("a.request_params")["commandId"].cast("string").alias("command_id"),
        col("a.request_params")["commandText"].cast("string").alias("statement_text")
    )
    .dropDuplicates(["notebook_id", "command_id"])
)

df_incidents = (
    download_events_filtered.alias("d")
    .join(
        command_events.alias("c"),
        on=["notebook_id", "command_id"],
        how="left"
    )
    .withColumn(
        "event_time",
        concat(
            date_format(
                from_utc_timestamp(col("event_time"), "America/Sao_Paulo"),
                "dd-MMM-yyyy hh:mm:ss a"
            ),
            lit(" GMT-3")
        )
    )
    .select(
        "event_time",
        "download_user_identity",
        "statement_text",
        "notebook_id",
        "command_id",
        "request_id",
        "event_id",
        "session_id"
    )
    .orderBy(col("event_time").asc())
)
```

### external-credentials-creation  
`databricks-config` · **ACTIVE** · DAILY · threshold=0 · risco=R1463 · área=Engenharia e Tecnologia KYP  

> Este teste possui o objetivo de verificar todas as ações de criação de credenciais externas registradas no audit log no dia anterior. Ele identifica quais usuários criaram credenciais e garante o monitoramento de operações sensíveis que podem conceder acesso a external locations, storage, conexões e integrações externas.

```python
df_incidents = (
    spark.table("system.access.audit")
    .where(
        (col("action_name") == lit("createCredential")) &
        (col("event_date") == date_sub(current_date(), 1))
    )
)
```

### unrevoked-tokens  
`databricks-config` · **ACTIVE** · MONTHLY · threshold=0 · risco=R1463 · área=Engenharia e Tecnologia KYP  

> Este teste possui o objetivo de verificar todos os tokens gerados no ambiente e identificar aqueles que permanecem ativos sem terem sido revogados. Ele cruza os registros de geração e revogação de tokens presentes no audit log, garantindo o monitoramento de credenciais sensíveis que ainda estão válidas e podem conceder/obter acessos indevidos no sistema.

```python
revokeDbToken = (
    spark.table("system.access.audit")
        .where(col("action_name") == lit("revokeDbToken"))
        .select(
            col("request_params.tokenHash").alias("token_hash"),
            col("event_date").alias("revoke_date")
        )
)

df_incidents = (
    spark.table("system.access.audit")
        .where(col("action_name") == lit("generateDbToken"))
        .withColumn(
            "token_expiration_dt",
            from_unixtime(col("request_params.tokenExpirationTime") / 1000).cast("timestamp")
        )
        .withColumn("token_hash", col("request_params.tokenHash"))
        .join(revokeDbToken, "token_hash", "left")
        .join(spark.table("system.access.workspaces_latest").select("workspace_id", "workspace_name", "workspace_url"), on="workspace_id", how="left")\
        .where(
            (col("revoke_date").isNull()) &
            (current_date() <= col("token_expiration_dt")) &
            (col("workspace_name").contains("prd"))
        )
)
```

### update-permission-assignment  
`databricks-config` · **ACTIVE** · MONTHLY · threshold=0 · risco=R446 · área=Engenharia e Tecnologia KYP  

> Este teste possui o objetivo de verificar todas as ações de atualização de permissões registradas no audit log, excluindo alterações realizadas por identidades de automação, como contas de terraform. Ele identifica quais usuários efetuaram modificações em permissões e garante o monitoramento de operações sensíveis que podem impactar o controle de acesso no ambiente.

```python
df_incidents = (
    spark.table("system.access.audit")
        .where(col("action_name") == lit("updatePermissionAssignment"))
        .where(~col("user_identity.email").contains("terraform"))
        .where((col("event_date") == date_sub(current_date(), 1)))
)
```

### anomalous-feeder-consumption-behavior  
`finance` · **ACTIVE** · WEEKLY · threshold=0 · risco=R252 · área=Tesouraria  

> O teste busca detectar padrões anômalos no consumo de feeders da SERPRO pelos clientes, sinalizando aumentos relevantes de consumo que possam representar elevação do risco de crédito.

```python
from pyspark.sql.window import Window

tb_assessments = spark.table("trusted.asset_assessment.tb_assessments")
tb_external_search = spark.table("trusted.asset_assessment.tb_external_search")

base = (
    tb_assessments.alias("ass")
    .join(
        tb_external_search.alias("ex"),
        col("ex.AssessmentId") == col("ass.AssessmentId"),
        "left"
    )
    .where(
        (col("ex.UrlParsedName") != "cache") &
        (col("ex.DataContractId").isin(
            "serpro_fetch_cnpj",
            "serpro_fetch_cnpj_pre_contract",
            "serpro_fetch_cpf",
            "serpro_fetch_cpfs",
            "serpro_fetch_cpf_pre_contract",
            "serpro_fetch_cpf_emissor",
            "serpro_fetch_nfe",
            "serpro_fetch_nfe_summary",
            "serpro_fetch_nfe_full"
        )) &
        (dayofweek(col("ass.CreatedAtDttm")).between(2, 6))
    )
    .select(
        col("ass.ClientCd").alias("ClientCd"),
        col("ass.CreatedAtDttm").alias("CreatedAtDttm"),
        when(
            col("ex.DataContractId").isin(
                "serpro_fetch_cpf",
                "serpro_fetch_cpfs",
                "serpro_fetch_cpf_pre_contract",
                "serpro_fetch_cpf_emissor"
            ),
            "CPF"
        )
        .when(
            col("ex.DataContractId").isin(
                "serpro_fetch_cnpj",
                "serpro_fetch_cnpj_pre_contract"
            ),
            "CNPJ"
        )
        .when(
            col("ex.DataContractId").isin(
                "serpro_fetch_nfe",
                "serpro_fetch_nfe_summary",
                "serpro_fetch_nfe_full"
            ),
            "NFE"
        )
        .alias("AssessmentTypeCd")
    )
)

base_semana = (
    base.withColumn(
        "CreatedAtDttm",
        next_day(
            date_trunc("week", col("CreatedAtDttm")),
            "FRI"
        )
    )
)

resultado = (
    base_semana
    .groupBy("ClientCd", "CreatedAtDttm", "AssessmentTypeCd")
    .agg(count("*").alias("AssessmentQty"))
    .withColumn(
        "AssessmentValueAmt",
        col("AssessmentQty") *
        when(col("AssessmentTypeCd") == "CPF", 0.13)
        .when(col("AssessmentTypeCd") == "CNPJ", 0.0990)
        .when(col("AssessmentTypeCd") == "NFE", 0.0293)
        .otherwise(0)
    )
)

resultado_final = (
    resultado
    .filter(col("ClientCd").isNotNull())
    .groupBy("ClientCd", "CreatedAtDttm")
    .agg(
        sum("AssessmentValueAmt").alias("AssessmentTotalValueAmt")
    )
)

w = Window.partitionBy("ClientCd").orderBy("CreatedAtDttm")
w_pico = w.rowsBetween(Window.unboundedPreceding, -1)

tabela_picos = (
    resultado_final
    .withColumn(
        "PrevPeakValueAmt",
        max("AssessmentTotalValueAmt").over(w_pico)
    )
    .withColumn(
        "PrevPeakValueAmt",
        when(
            col("PrevPeakValueAmt").isNull(),
            col("AssessmentTotalValueAmt")
        ).otherwise(col("PrevPeakValueAmt"))
    )
    .withColumn(
        "VariationPct",
        (col("AssessmentTotalValueAmt") - col("PrevPeakValueAmt")) /
        col("PrevPeakValueAmt")
    )
    .withColumn(
        "AlertInd",
        (col("VariationPct") >= 0.25) &
        (col("AssessmentTotalValueAmt") >= 25000)
    )
)


total_avaliacoes = (
    resultado
    .groupBy("ClientCd", "CreatedAtDttm")
    .agg(
        sum("AssessmentQty").alias("AssessmentTotalQty")
    )
)

df_incidents = (
    tabela_picos
    .filter(col("AlertInd") == True)
    .filter(
        col("CreatedAtDttm") == next_day(
            date_trunc("week", current_date()),
            "FRI"
        )
    )
    .join(
        total_avaliacoes,
        ["ClientCd", "CreatedAtDttm"],
        "left"
    )
    .select(
        "ClientCd",
        "CreatedAtDttm",
        "AssessmentTotalValueAmt",
        "AssessmentTotalQty"
    )
    .orderBy("ClientCd", "CreatedAtDttm")
)
```

### bypass-in-sonar-testing-policies  
`policy-devops` · **ACTIVE** · DAILY · threshold=0 · risco=R292 · área=Infraestrutura  

> O objetivo deste teste é identificar, quem desativou ou deletou regras (policy) de verificação de código no Sonar. Essas regras funcionam como “proteções automáticas”: quando são removidas, o sistema pode deixar de sinalizar erros de código e/ou falhas de segurança, aumentando o risco de códigos problemáticos seguirem adiante sem o devido controle.

```python
df_incidents = (
    spark.table("trusted.infracorp.tb_sonar_policy_history")
    .withColumn(
        "event_date",
        to_date(col("IngestedAtDttm"))
    )
    .where(
        (col("ActionName").isin("disabled", "deleted")) &
        (col("event_date") == date_sub(current_date(), 1))
    )
    .groupBy(
        col("UserEmailTxt"),
        col("ProjectName"),
        col("RepositoryName")
    )
    .agg(
        sum(
            when(col("ActionName") == "disabled", 1).otherwise(0)
        ).alias("disabled_count"),
        sum(
            when(col("ActionName") == "deleted", 1).otherwise(0)
        ).alias("deleted_count")
    )
)
```

