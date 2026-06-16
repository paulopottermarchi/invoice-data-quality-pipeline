-- ============================================================
-- BORDERO E COMISSIONAMENTO
-- Calcula comissão proporcional por pagamento
-- ============================================================

-- ============================================================
-- PASSO 1: materializa casos com índice
-- ============================================================
DROP TABLE IF EXISTS #casos;
SELECT
   c.case_id,
   c.ref_number,
   c.client_ref_number,
   c.case_statute_id,
   c.debtor_id
INTO #casos
FROM debthor_dbs_interface.dtdi.[case] c
WHERE c.client_id = 516;

CREATE CLUSTERED INDEX ix_casos_case_id ON #casos(case_id);

-- ============================================================
-- PASSO 2: materializa base_raw com todas as strings processadas
-- ============================================================
DROP TABLE IF EXISTS #base_raw;
SELECT  
   c.case_id,
   c.ref_number,
   c.client_ref_number,

   TRY_CAST(ca.case_attribute_value AS INT) AS live_dpd,

   i.invoice_id,
   i.invoice_number,
   CAST(i.update_date AS DATE)      AS update_date,
   i.original_capital               AS invoice_original_capital,
   i.actual_capital                 AS invoice_actual_capital,

   LTRIM(RTRIM(
       CASE 
           WHEN i.description LIKE '%NN:%' THEN
               SUBSTRING(i.description, CHARINDEX('NN:', i.description) + 3,
                   CHARINDEX(CHAR(10), i.description + CHAR(10), CHARINDEX('NN:', i.description)) 
                   - (CHARINDEX('NN:', i.description) + 3))
           WHEN i.description LIKE '%Titulo:%' THEN
               SUBSTRING(i.description, CHARINDEX('Titulo:', i.description) + 7,
                   CHARINDEX(CHAR(10), i.description + CHAR(10), CHARINDEX('Titulo:', i.description)) 
                   - (CHARINDEX('Titulo:', i.description) + 7))
       END
   )) AS codigo_titulo,

   LTRIM(RTRIM(
       SUBSTRING(i.description,
           CHARINDEX('Competencia:', i.description) + LEN('Competencia:'),
           PATINDEX('%[' + CHAR(13) + CHAR(10) + ']%',
               SUBSTRING(i.description,
                   CHARINDEX('Competencia:', i.description) + LEN('Competencia:'), 50)
               + CHAR(13)) - 1)
   )) AS competencia_raw,

   LTRIM(RTRIM(
       SUBSTRING(i.description,
           CHARINDEX('Vencimento:', i.description) + LEN('Vencimento:'),
           PATINDEX('%[' + CHAR(13) + CHAR(10) + ']%',
               SUBSTRING(i.description,
                   CHARINDEX('Vencimento:', i.description) + LEN('Vencimento:'), 50)
               + CHAR(13)) - 1)
   )) AS vencimento_raw,

   TRY_CAST(
       LEFT(
           LTRIM(SUBSTRING(i.description, CHARINDEX('DPD:', i.description) + 4, 20)),
           PATINDEX('%[^0-9\-]%',
               LTRIM(SUBSTRING(i.description, CHARINDEX('DPD:', i.description) + 4, 20)) + 'X'
           ) - 1
       )
   AS INT) AS dpd_invoice

INTO #base_raw
FROM #casos c
INNER JOIN debthor_dbs_interface.dtdi.invoice i
   ON i.case_id = c.case_id
LEFT JOIN debthor_dbs_interface.dtdi.case_attribute ca 
   ON ca.case_id = c.case_id
  AND ca.case_attribute_type_id = 560;

CREATE CLUSTERED INDEX ix_base_raw ON #base_raw(case_id, invoice_id);

-- ============================================================
-- PASSO 3: converte datas e aplica correção de inversão
-- ============================================================
DROP TABLE IF EXISTS #base;
SELECT
   r.case_id,
   r.ref_number,
   r.client_ref_number,
   r.live_dpd,
   r.invoice_id,
   r.invoice_number,
   r.update_date,
   r.invoice_original_capital,
   r.invoice_actual_capital,
   r.codigo_titulo,
   r.dpd_invoice,

   TRY_CONVERT(DATE, r.competencia_raw, 104) AS competencia_description,
   TRY_CONVERT(DATE, r.vencimento_raw,  104) AS vencimento_original,

   COALESCE(
       TRY_CONVERT(DATE,
           CASE
               WHEN SUBSTRING(r.vencimento_raw, 4, 2) <> SUBSTRING(r.competencia_raw, 4, 2)
               THEN
                   SUBSTRING(r.vencimento_raw, 4, 2) + '.' +
                   SUBSTRING(r.vencimento_raw, 1, 2) + '.' +
                   SUBSTRING(r.vencimento_raw, 7, 4)
               ELSE r.vencimento_raw
           END
       , 104),
       TRY_CONVERT(DATE,
           SUBSTRING(r.competencia_raw, 4, 2) + '.' +
           SUBSTRING(r.competencia_raw, 1, 2) + '.' +
           SUBSTRING(r.competencia_raw, 7, 4)
       , 104)
   ) AS vencimento_description

INTO #base
FROM #base_raw r;

CREATE CLUSTERED INDEX ix_base ON #base(case_id, invoice_id);

-- ============================================================
-- PASSO 4: deduplica por codigo_titulo mantendo DPD >= 0
-- ============================================================
DROP TABLE IF EXISTS #base_dedup;
SELECT *
INTO #base_dedup
FROM (
   SELECT
       b.*,
       ROW_NUMBER() OVER (
           PARTITION BY b.case_id, b.codigo_titulo
           ORDER BY
               CASE WHEN b.dpd_invoice >= 0 THEN 0 ELSE 1 END,
               b.dpd_invoice DESC
       ) AS rn_titulo
   FROM #base b
   WHERE b.codigo_titulo IS NOT NULL AND b.codigo_titulo <> ''
) x
WHERE rn_titulo = 1

UNION ALL

SELECT b.*, 1
FROM #base b
WHERE b.codigo_titulo IS NULL OR b.codigo_titulo = '';

CREATE CLUSTERED INDEX ix_base_dedup ON #base_dedup(case_id, invoice_id);

-- ============================================================
-- PASSO 5: devedores
-- ============================================================
DROP TABLE IF EXISTS #devedores;
SELECT
   c.case_id,
   c.case_statute_id,
   p.persons_born_number,
   CASE 
       WHEN LEN(REPLACE(REPLACE(REPLACE(REPLACE(p.persons_born_number, '.', ''), '-', ''), '/', ''), ' ', '')) > 11 
           THEN 'CNPJ'
       ELSE 'CPF'
   END AS documento_tipo
INTO #devedores
FROM #casos c
INNER JOIN debthor_dbs_interface.dtdi.debtor d
   ON d.debtor_id = c.debtor_id
INNER JOIN debthor_dbs_interface.dtdi.Persons p
   ON p.persons_id = d.Persons_id;

CREATE CLUSTERED INDEX ix_devedores ON #devedores(case_id);

-- ============================================================
-- PASSO 6: ptp
-- ============================================================
DROP TABLE IF EXISTS #ptp;
SELECT
   case_id,
   MAX(promise_date)    AS promise_date,
   MAX(promise_capital) AS promise_capital,
   MAX(contact_date)    AS contact_date
INTO #ptp
FROM debthor_dbs_interface.reports.vw_ptp_ptpr_overview
WHERE client_id = 516
GROUP BY case_id;

CREATE CLUSTERED INDEX ix_ptp ON #ptp(case_id);

-- ============================================================
-- PASSO 6B: tabela de comissão por DPD do cliente 516
-- ============================================================
DROP TABLE IF EXISTS #comissao;
SELECT
   dpd_on_payment_day_from,
   dpd_on_payment_day_to,
   commission_rate
INTO #comissao
FROM debthor_dbs_interface.dtdi.client_settings_commission_by_dpd
WHERE client_id = 516
 AND CAST(GETDATE() AS DATE) BETWEEN date_from AND date_to;

CREATE CLUSTERED INDEX ix_comissao ON #comissao(dpd_on_payment_day_from, dpd_on_payment_day_to);

-- ============================================================
-- PASSO 7: pagamentos apenas do mês atual
-- ============================================================
DROP TABLE IF EXISTS #pagamentos;
SELECT
   p.case_id,
   p.payment_id,
   p.payment_date,
   p.payed_capital,
   p.payment_insert_date,
   p.payment_insert_time,
   p.pay_insert_filename,
   p.payment_insert_user
INTO #pagamentos
FROM debthor_dbs_interface.dtdi.vw_cases_payment_information p
WHERE EXISTS (SELECT 1 FROM #casos c WHERE c.case_id = p.case_id)
 AND MONTH(p.payment_date) = MONTH(GETDATE())
 AND YEAR(p.payment_date)  = YEAR(GETDATE());

CREATE CLUSTERED INDEX ix_pagamentos ON #pagamentos(case_id, payment_date);

-- ============================================================
-- PASSO 8: base_agregada
-- ============================================================
DROP TABLE IF EXISTS #base_agregada;
SELECT
   b.case_id,
   b.ref_number,
   b.client_ref_number,
   b.invoice_id,
   b.invoice_number,

   SUM(b.invoice_actual_capital)                              AS actual_capital_total,
   SUM(b.invoice_original_capital)                            AS original_capital_total,
   SUM(b.invoice_original_capital - b.invoice_actual_capital) AS valor_pago,
   SUM(b.invoice_original_capital - b.invoice_actual_capital) AS valor_pago_invoice,

   CASE 
       WHEN SUM(b.invoice_actual_capital) = 0 
           THEN 'PAGO TOTAL'
       WHEN SUM(b.invoice_actual_capital) < SUM(b.invoice_original_capital) 
           THEN 'PAGO PARCIAL'
       ELSE 'EM ABERTO'
   END AS status_pagamento,

   MAX(b.live_dpd)                AS live_dpd,
   MAX(b.dpd_invoice)             AS dpd_invoice,
   MAX(b.competencia_description) AS competencia,
   MAX(b.vencimento_original)     AS vencimento_original,
   MAX(b.vencimento_description)  AS data_vencimento,
   MAX(b.update_date)             AS update_date,

   CASE
       WHEN MAX(b.vencimento_description) IS NOT NULL
           THEN DATEDIFF(DAY, MAX(b.vencimento_description), CAST(GETDATE() AS DATE))
       WHEN MAX(b.dpd_invoice) IS NULL
           THEN MAX(b.live_dpd)
       ELSE MAX(b.dpd_invoice)
   END AS dpd_final,

   STRING_AGG(b.codigo_titulo, ',') AS codigo_titulo,

   CASE
       WHEN DATEDIFF(DAY, MAX(b.vencimento_description), CAST(GETDATE() AS DATE)) BETWEEN 1    AND 30    THEN 'A - 1 A 30 | 6%'
       WHEN DATEDIFF(DAY, MAX(b.vencimento_description), CAST(GETDATE() AS DATE)) BETWEEN 31   AND 60    THEN 'B - 31 A 60 | 11%'
       WHEN DATEDIFF(DAY, MAX(b.vencimento_description), CAST(GETDATE() AS DATE)) BETWEEN 61   AND 90    THEN 'C - 61 A 90 | 13%'
       WHEN DATEDIFF(DAY, MAX(b.vencimento_description), CAST(GETDATE() AS DATE)) BETWEEN 91   AND 120   THEN 'D - 91 A 120 | 15%'
       WHEN DATEDIFF(DAY, MAX(b.vencimento_description), CAST(GETDATE() AS DATE)) BETWEEN 121  AND 180   THEN 'E - 121 A 180 | 18%'
       WHEN DATEDIFF(DAY, MAX(b.vencimento_description), CAST(GETDATE() AS DATE)) BETWEEN 181  AND 240   THEN 'F - 181 A 240 | 20%'
       WHEN DATEDIFF(DAY, MAX(b.vencimento_description), CAST(GETDATE() AS DATE)) BETWEEN 241  AND 270   THEN 'G - 241 A 270 | 21%'
       WHEN DATEDIFF(DAY, MAX(b.vencimento_description), CAST(GETDATE() AS DATE)) BETWEEN 271  AND 300   THEN 'H - 271 A 300 | 22%'
       WHEN DATEDIFF(DAY, MAX(b.vencimento_description), CAST(GETDATE() AS DATE)) BETWEEN 301  AND 330   THEN 'I - 301 A 330 | 24%'
       WHEN DATEDIFF(DAY, MAX(b.vencimento_description), CAST(GETDATE() AS DATE)) BETWEEN 331  AND 360   THEN 'J - 331 A 360 | 28%'
       WHEN DATEDIFF(DAY, MAX(b.vencimento_description), CAST(GETDATE() AS DATE)) BETWEEN 361  AND 540   THEN 'K - 361 A 540 | 30%'
       WHEN DATEDIFF(DAY, MAX(b.vencimento_description), CAST(GETDATE() AS DATE)) BETWEEN 541  AND 720   THEN 'L - 541 A 720 | 35%'
       WHEN DATEDIFF(DAY, MAX(b.vencimento_description), CAST(GETDATE() AS DATE)) BETWEEN 721  AND 1440  THEN 'M - 721 A 1440 | 40%'
       WHEN DATEDIFF(DAY, MAX(b.vencimento_description), CAST(GETDATE() AS DATE)) BETWEEN 1441 AND 1800  THEN 'N - 1441 A 1800 | 40%'
       WHEN DATEDIFF(DAY, MAX(b.vencimento_description), CAST(GETDATE() AS DATE)) BETWEEN 1801 AND 99999 THEN 'O - 1801 A 99999 | 40%'
       ELSE ''
   END AS fase

INTO #base_agregada
FROM #base_dedup b
GROUP BY
   b.case_id,
   b.ref_number,
   b.client_ref_number,
   b.invoice_id,
   b.invoice_number;

CREATE CLUSTERED INDEX ix_ba ON #base_agregada(case_id, invoice_id);

-- ============================================================
-- PASSO 9: total pago por case_id + update_date
-- ============================================================
DROP TABLE IF EXISTS #total_pago_dia;
SELECT
   case_id,
   update_date             AS update_dia,
   SUM(valor_pago_invoice) AS valor_pago_total_dia
INTO #total_pago_dia
FROM #base_agregada
WHERE valor_pago_invoice > 0
GROUP BY case_id, update_date;

CREATE CLUSTERED INDEX ix_tpd ON #total_pago_dia(case_id, update_dia);

-- ============================================================
-- PASSO 10A: numera pagamentos por caso em ordem cronológica
-- ============================================================
DROP TABLE IF EXISTS #pg_ranked;
SELECT
   pg.case_id,
   pg.payment_id,
   pg.payment_date,
   pg.payed_capital,
   pg.payment_insert_date,
   pg.payment_insert_time,
   pg.pay_insert_filename,
   pg.payment_insert_user,
   ROW_NUMBER() OVER (
       PARTITION BY pg.case_id
       ORDER BY pg.payment_date ASC, pg.payment_id ASC
   ) AS rn_pg
INTO #pg_ranked
FROM #pagamentos pg;

CREATE CLUSTERED INDEX ix_pg_ranked ON #pg_ranked(case_id, rn_pg);

-- ============================================================
-- PASSO 10B: numera invoices com desconto por caso em ordem
-- ============================================================
DROP TABLE IF EXISTS #inv_ranked;
SELECT
   ba.invoice_id,
   ba.case_id,
   ba.invoice_number,
   ba.update_date,
   ROW_NUMBER() OVER (
       PARTITION BY ba.case_id
       ORDER BY ba.invoice_number ASC
   ) AS rn_inv
INTO #inv_ranked
FROM #base_agregada ba
WHERE ba.valor_pago_invoice > 0;

CREATE CLUSTERED INDEX ix_inv_ranked ON #inv_ranked(case_id, rn_inv);

-- ============================================================
-- PASSO 10C: match sequencial payment x invoice
-- ============================================================
DROP TABLE IF EXISTS #invoice_payment;
SELECT
   inv.invoice_id,
   inv.case_id,
   pg.payment_id,
   pg.payment_date,
   pg.payed_capital,
   pg.payment_insert_date,
   pg.payment_insert_time,
   pg.pay_insert_filename,
   pg.payment_insert_user
INTO #invoice_payment
FROM #pg_ranked pg
INNER JOIN #inv_ranked inv
   ON inv.case_id = pg.case_id
  AND inv.rn_inv  = pg.rn_pg;

CREATE CLUSTERED INDEX ix_ip ON #invoice_payment(invoice_id);

DROP TABLE IF EXISTS #pg_ranked;
DROP TABLE IF EXISTS #inv_ranked;

-- ============================================================
-- SELECT FINAL
-- ============================================================
SELECT
   b.case_id,
   b.ref_number,
   b.client_ref_number,
   b.invoice_id,
   b.invoice_number,
   b.original_capital_total,
   b.actual_capital_total,
   b.valor_pago,
   b.status_pagamento,
   b.competencia,
   b.vencimento_original,
   b.data_vencimento,
   b.codigo_titulo,
   b.dpd_invoice,
   b.live_dpd,
   b.dpd_final,
   b.fase,
   b.update_date,

   p.promise_date,
   p.promise_capital,
   p.contact_date,

   d.case_statute_id,
   d.persons_born_number,
   d.documento_tipo,

   pg.payment_id,
   pg.payment_date,
   pg.payed_capital                                                            AS payed_capital_original,
   ROUND(
       pg.payed_capital * (b.valor_pago_invoice / tpd.valor_pago_total_dia)
   , 2)                                                                        AS payed_capital_proporcional,

   cm.commission_rate,
   ROUND(
       ROUND(pg.payed_capital * (b.valor_pago_invoice / tpd.valor_pago_total_dia), 2)
       * cm.commission_rate
   , 2)                                                                        AS valor_comissao,

   pg.payment_insert_date,
   pg.payment_insert_time,
   pg.pay_insert_filename,
   pg.payment_insert_user

FROM #base_agregada b
LEFT JOIN #ptp p
   ON p.case_id = b.case_id
LEFT JOIN #devedores d
   ON d.case_id = b.case_id
LEFT JOIN #invoice_payment pg
   ON pg.invoice_id = b.invoice_id
LEFT JOIN #total_pago_dia tpd
   ON tpd.case_id    = b.case_id
  AND tpd.update_dia = b.update_date
LEFT JOIN #comissao cm
   ON b.dpd_final BETWEEN cm.dpd_on_payment_day_from AND cm.dpd_on_payment_day_to

WHERE
   b.valor_pago > 0
   AND (
       (MONTH(b.update_date) = MONTH(GETDATE()) AND YEAR(b.update_date) = YEAR(GETDATE()))
       OR pg.payment_id IS NOT NULL
   )

ORDER BY b.case_id, b.invoice_number;

-- ============================================================
-- LIMPEZA
-- ============================================================
DROP TABLE IF EXISTS #casos;
DROP TABLE IF EXISTS #base_raw;
DROP TABLE IF EXISTS #base;
DROP TABLE IF EXISTS #base_dedup;
DROP TABLE IF EXISTS #devedores;
DROP TABLE IF EXISTS #ptp;
DROP TABLE IF EXISTS #comissao;
DROP TABLE IF EXISTS #pagamentos;
DROP TABLE IF EXISTS #base_agregada;
DROP TABLE IF EXISTS #total_pago_dia;
DROP TABLE IF EXISTS #invoice_payment;
