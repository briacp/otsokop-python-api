
SELECT -- noqa: ST06
    p.name,
    p.id,
    p.barcode,
    pc.name AS category,
    p.product_rack_code,
    p.base_price AS prix_base_ht,
    p.theoritical_price AS prix_vente_ttc,
    t.amount / 100 AS tax,
    coeff_1.name AS coeff_1,
    coeff_1.value AS coeff_1_val,
    coeff_1.operation_type = 'fixed' AS c1_fixed,
    coeff_2.name AS coeff_2,
    coeff_2.value AS coeff_2_val,
    coeff_2.operation_type = 'fixed' AS c2_fixed,
    coeff_3.name AS coeff_3,
    coeff_3.value AS coeff_3_val,
    coeff_3.operation_type = 'fixed' AS c3_fixed,
    coeff_4.name AS coeff_4,
    coeff_4.value AS coeff_4_val,
    coeff_4.operation_type = 'fixed' AS c4_fixed,
    coeff_5.name AS coeff_5,
    coeff_5.value AS coeff_5_val,
    coeff_5.operation_type = 'fixed' AS c5_fixed,
    p.sale_ok,
    p.active,
    p.deref,
    concat_ws(
        ',',
        coeff_1.name,
        coeff_2.name,
        coeff_3.name,
        coeff_4.name,
        coeff_5.name
    ) AS coefficients
FROM
    product AS p
INNER JOIN product_category AS pc
    ON
        p.product_category_id = pc.id
INNER JOIN account_tax AS t
    ON
        p.tax_id = t.id
LEFT JOIN product_coefficient AS coeff_1
    ON
        p.coeff1_id = coeff_1.id
LEFT JOIN product_coefficient AS coeff_2
    ON
        p.coeff2_id = coeff_2.id
LEFT JOIN product_coefficient AS coeff_3
    ON
        p.coeff3_id = coeff_3.id
LEFT JOIN product_coefficient AS coeff_4
    ON
        p.coeff4_id = coeff_4.id
LEFT JOIN product_coefficient AS coeff_5
    ON
        p.coeff5_id = coeff_5.id
WHERE
    TRUE
    AND p.active = 1
    AND p.sale_ok = 1
    AND p.base_price IS NOT NULL
ORDER BY
    p.product_rack_code;
