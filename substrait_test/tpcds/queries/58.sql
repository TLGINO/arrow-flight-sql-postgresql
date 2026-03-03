SELECT "t5"."I_ITEM_ID0" AS "ITEM_ID", "t5"."$f1" AS "SS_ITEM_REV", "t5"."$f1" / ("t5"."$f1" + "t12"."$f1" + "t19"."$f1") / 3.000000 * 100.000000 AS "SS_DEV", "t12"."$f1" AS "CS_ITEM_REV", "t12"."$f1" / ("t5"."$f1" + "t12"."$f1" + "t19"."$f1") / 3.000000 * 100.000000 AS "CS_DEV", "t19"."$f1" AS "WS_ITEM_REV", "t19"."$f1" / ("t5"."$f1" + "t12"."$f1" + "t19"."$f1") / 3.000000 * 100.000000 AS "WS_DEV", ("t5"."$f1" + "t12"."$f1" + "t19"."$f1") / 3.00 AS "AVERAGE"
FROM (SELECT "ITEM"."I_ITEM_ID" AS "I_ITEM_ID0", SUM("STORE_SALES"."SS_EXT_SALES_PRICE") AS "$f1"
FROM "tpcds"."STORE_SALES",
"tpcds"."ITEM",
"tpcds"."DATE_DIM"
WHERE "STORE_SALES"."SS_ITEM_SK" = "ITEM"."I_ITEM_SK" AND "DATE_DIM"."D_DATE" IN (SELECT "D_DATE" AS "D_DATE0"
FROM "tpcds"."DATE_DIM"
WHERE "D_WEEK_SEQ" = (((SELECT "D_WEEK_SEQ" AS "D_WEEK_SEQ0"
FROM "tpcds"."DATE_DIM"
WHERE "D_DATE" = 'date(1998+"-01-01",1998+"-07-24",sales)')))) AND "STORE_SALES"."SS_SOLD_DATE_SK" = "DATE_DIM"."D_DATE_SK"
GROUP BY "ITEM"."I_ITEM_ID") AS "t5",
(SELECT "ITEM0"."I_ITEM_ID" AS "I_ITEM_ID0", SUM("CATALOG_SALES"."CS_EXT_SALES_PRICE") AS "$f1"
FROM "tpcds"."CATALOG_SALES",
"tpcds"."ITEM" AS "ITEM0",
"tpcds"."DATE_DIM" AS "DATE_DIM2"
WHERE "CATALOG_SALES"."CS_ITEM_SK" = "ITEM0"."I_ITEM_SK" AND "DATE_DIM2"."D_DATE" IN (SELECT "D_DATE" AS "D_DATE0"
FROM "tpcds"."DATE_DIM"
WHERE "D_WEEK_SEQ" = (((SELECT "D_WEEK_SEQ" AS "D_WEEK_SEQ0"
FROM "tpcds"."DATE_DIM"
WHERE "D_DATE" = 'date(1998+"-01-01",1998+"-07-24",sales)')))) AND "CATALOG_SALES"."CS_SOLD_DATE_SK" = "DATE_DIM2"."D_DATE_SK"
GROUP BY "ITEM0"."I_ITEM_ID") AS "t12",
(SELECT "ITEM1"."I_ITEM_ID" AS "I_ITEM_ID0", SUM("WEB_SALES"."WS_EXT_SALES_PRICE") AS "$f1"
FROM "tpcds"."WEB_SALES",
"tpcds"."ITEM" AS "ITEM1",
"tpcds"."DATE_DIM" AS "DATE_DIM5"
WHERE "WEB_SALES"."WS_ITEM_SK" = "ITEM1"."I_ITEM_SK" AND "DATE_DIM5"."D_DATE" IN (SELECT "D_DATE" AS "D_DATE0"
FROM "tpcds"."DATE_DIM"
WHERE "D_WEEK_SEQ" = (((SELECT "D_WEEK_SEQ" AS "D_WEEK_SEQ0"
FROM "tpcds"."DATE_DIM"
WHERE "D_DATE" = 'date(1998+"-01-01",1998+"-07-24",sales)')))) AND "WEB_SALES"."WS_SOLD_DATE_SK" = "DATE_DIM5"."D_DATE_SK"
GROUP BY "ITEM1"."I_ITEM_ID") AS "t19"
WHERE "t5"."I_ITEM_ID0" = "t12"."I_ITEM_ID0" AND ("t5"."I_ITEM_ID0" = "t19"."I_ITEM_ID0" AND "t5"."$f1" >= 0.9 * "t12"."$f1") AND ("t5"."$f1" <= 1.1 * "t12"."$f1" AND "t5"."$f1" >= 0.9 * "t19"."$f1" AND ("t5"."$f1" <= 1.1 * "t19"."$f1" AND "t12"."$f1" >= 0.9 * "t5"."$f1")) AND ("t12"."$f1" <= 1.1 * "t5"."$f1" AND ("t12"."$f1" >= 0.9 * "t19"."$f1" AND "t12"."$f1" <= 1.1 * "t19"."$f1") AND ("t19"."$f1" >= 0.9 * "t5"."$f1" AND "t19"."$f1" <= 1.1 * "t5"."$f1" AND ("t19"."$f1" >= 0.9 * "t12"."$f1" AND "t19"."$f1" <= 1.1 * "t12"."$f1")))
ORDER BY "t5"."I_ITEM_ID0", "t5"."$f1"
FETCH NEXT 100 ROWS ONLY
