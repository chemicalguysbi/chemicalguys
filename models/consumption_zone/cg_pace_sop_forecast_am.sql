{{
       config(
             materialized='table',
             tags = 'cg_pace_sop_forecast_am'
         )
   }}


SELECT
  a.*,
  b.no_of AS count_of_category,
  CASE
    WHEN a.main_class_category = 'Wholesale Distribution' THEN a.amount
  ELSE
  b.calculated_amount
END
  AS calculated_amount
FROM (
  SELECT
    b.main_class_category,
    coalesce (a.main_class_sub_category,
      b.main_class_sub_category)main_class_sub_category,
    amount,
    main_date
  FROM
    --`cg-gbq-p.staging_zone.cg_pace_sop_forecast` b
    {{ ref('cg_pace_sop_forecast') }} b
  LEFT JOIN (
    SELECT
      DISTINCT main_class_category,
      COALESCE(main_class_sub_category,main_class) main_class_sub_category
    FROM
      --`cg-gbq-p.consumption_zone.cg_customer_class`
      {{ ref('cg_customer_class') }}
    WHERE
      main_class_category <>'Wholesale Distribution'
      AND (main_class_sub_category IN ('Fee',
          'Owned Store Sales',
          'Sales To Franchise',
          'Amazon Canada',
          'Amazon US',
          'eBay',
          'AUS Website',
          'UK Website',
          'CAN Website',
          'US Websites')
        OR main_class IN ('Fee',
          'Owned Store Sales',
          'Sales To Franchise',
          'Amazon Canada',
          'Amazon US',
          'eBay',
          'AUS Website',
          'UK Website',
          'CAN Website',
          'US Websites'))
    ORDER BY
      1,
      2)a
  ON
    b.main_class_category = a.main_class_category
  ORDER BY
    1,
    4 ASC)a
LEFT JOIN (
  SELECT
    main_class_category,
    COUNT(*) AS no_of,
    MAX(amount)final_amount,
    MAX(amount)/COUNT(*) AS calculated_amount,
    main_date
  FROM (
    SELECT
      b.main_class_category,
      coalesce (a.main_class_sub_category,
        b.main_class_sub_category)main_class_sub_category,
      amount,
      main_date
    FROM
      --`cg-gbq-p.staging_zone.cg_pace_sop_forecast` b
         {{ ref('cg_pace_sop_forecast') }} b
    LEFT JOIN (
      SELECT
        DISTINCT main_class_category,
        COALESCE(main_class_sub_category,main_class) main_class_sub_category
      FROM
        --`cg-gbq-p.consumption_zone.cg_customer_class`
        {{ ref('cg_customer_class') }}
      WHERE
        main_class_category <>'Wholesale Distribution'
        AND (main_class_sub_category IN ('Fee',
            'Owned Store Sales',
            'Sales To Franchise',
            'Amazon Canada',
            'Amazon US',
            'eBay',
            'AUS Website',
            'UK Website',
            'CAN Website',
            'US Websites')
          OR main_class IN ('Fee',
            'Owned Store Sales',
            'Sales To Franchise',
            'Amazon Canada',
            'Amazon US',
            'eBay',
            'AUS Website',
            'UK Website',
            'CAN Website',
            'US Websites'))
      ORDER BY
        1,
        2)a
    ON
      b.main_class_category = a.main_class_category
    ORDER BY
      1,
      4 ASC)
  GROUP BY
    main_class_category,
    main_date)b
ON
  a.main_class_category = b.main_class_category
  AND a.main_date = b.main_date
ORDER BY
  1