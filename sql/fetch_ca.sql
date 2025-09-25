WITH base AS (
    SELECT
        p.id AS product_id,
        p.name AS product_name,
        c.id AS category_id,
        c.name AS category_name,
        concat_ws(" - ", product_rack.code, product_rack.name) AS rack,
        quarter(po.date_order) AS q,
        sum(pod.price_subtotal_incl) AS amt_ttc
    FROM
        pos_order_detail AS pod
    INNER JOIN pos_order AS po
        ON
            pod.pos_order_id = po.id
    INNER JOIN product AS p
        ON
            pod.product_id = p.id
    INNER JOIN product_category AS c
        ON
            p.product_category_id = c.id
    INNER JOIN product_rack
        ON
            p.product_rack_code = product_rack.code
    WHERE
        po.state = "done"
        AND po.date_order >= "2025-01-01"
        AND po.date_order < "2025-07-01"
        AND pod.price_subtotal_incl > 0
    GROUP BY
        p.id,
        p.name,
        c.id,
        c.name,
        q
),

pivot AS (
    SELECT
        product_id,
        product_name,
        category_name,
        rack,
        sum(CASE WHEN q = 1 THEN amt_ttc ELSE 0 END) AS t1_ttc,
        sum(CASE WHEN q = 2 THEN amt_ttc ELSE 0 END) AS t2_ttc,
        sum(CASE WHEN q = 3 THEN amt_ttc ELSE 0 END) AS t3_ttc,
        sum(CASE WHEN q = 4 THEN amt_ttc ELSE 0 END) AS t4_ttc,
        sum(amt_ttc) AS sem_ttc
    FROM
        base
    GROUP BY
        product_id,
        product_name,
        category_name,
        rack
),

ranked AS (
    SELECT
        p.*,
        row_number() OVER (
            ORDER BY p.sem_ttc DESC
        ) AS rang,
        sum(p.t1_ttc) OVER (ORDER BY p.sem_ttc DESC) AS cum_t1_running,
        sum(p.t2_ttc) OVER (ORDER BY p.sem_ttc DESC) AS cum_t2_running,
        sum(p.t3_ttc) OVER (ORDER BY p.sem_ttc DESC) AS cum_t3_running,
        sum(p.t4_ttc) OVER (ORDER BY p.sem_ttc DESC) AS cum_t4_running,
        sum(p.sem_ttc) OVER (ORDER BY p.sem_ttc DESC) AS cum_sem_running
    FROM
        pivot AS p
),

totals AS (
    SELECT
        sum(CASE WHEN q = 1 THEN amt_ttc ELSE 0 END) AS total_t1,
        sum(CASE WHEN q = 2 THEN amt_ttc ELSE 0 END) AS total_t2,
        sum(CASE WHEN q = 3 THEN amt_ttc ELSE 0 END) AS total_t3,
        sum(CASE WHEN q = 4 THEN amt_ttc ELSE 0 END) AS total_t4,
        sum(amt_ttc) AS total_sem
    FROM
        base
)

SELECT
    r.rang,
    r.rack,
    r.category_name AS sous_categorie,
    r.product_name AS produit,
    r.sem_ttc AS vente_ttc,
    r.t1_ttc AS vente_ttc_t1,
    r.t2_ttc AS vente_ttc_t2,
    r.t3_ttc AS vente_ttc_t3,
    r.t4_ttc AS vente_ttc_t4,
    r.sem_ttc / nullif(t.total_sem, 0) * 100 AS vente_ttc_pc,
    r.cum_sem_running / nullif(t.total_sem, 0) * 100 AS vente_ttc_cum_sum,
    r.t1_ttc / nullif(t.total_t1, 0) * 100 AS vente_ttc_t1_pc,
    r.cum_t1_running / nullif(t.total_t1, 0) * 100 AS vente_ttc_t1_cum_sum,
    r.t2_ttc / nullif(t.total_t2, 0) * 100 AS vente_ttc_t2_pc,
    r.cum_t2_running / nullif(t.total_t2, 0) * 100 AS vente_ttc_t2_cum_sum,
    r.t3_ttc / nullif(t.total_t3, 0) * 100 AS vente_ttc_t3_pc,
    r.cum_t3_running / nullif(t.total_t3, 0) * 100 AS vente_ttc_t3_cum_sum,
    r.t4_ttc / nullif(t.total_t4, 0) * 100 AS vente_ttc_t4_pc,
    r.cum_t4_running / nullif(t.total_t4, 0) * 100 AS vente_ttc_t4_cum_sum
FROM
    ranked AS r, totals AS t
ORDER BY
    r.rang ASC,
    vente_ttc DESC;
