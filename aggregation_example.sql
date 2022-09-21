CREATE TABLE product_nodes
(
    node_id                VARCHAR(36)  DEFAULT uuid()
  , node_natural_key       INTEGER      NOT NULL
  , node_name              VARCHAR(100) NOT NULL
  , level_name             VARCHAR(100) NOT NULL
  , parent_node_id         VARCHAR(36)
--
  , CONSTRAINT product_nodes_pk PRIMARY KEY (node_id)
  , CONSTRAINT product_nodes_uk_1 UNIQUE (node_natural_key)
  , CONSTRAINT product_nodes_self_fk FOREIGN KEY (parent_node_id)
        REFERENCES product_nodes (node_id)
)
;


-- Root (Top) Node (has no parent node)
INSERT INTO product_nodes (node_natural_key, node_name, level_name, parent_node_id)
VALUES (0, 'All Products', 'Total Products', NULL);

-- Produce Category Level Node
INSERT INTO product_nodes (node_natural_key, node_name, level_name, parent_node_id)
VALUES (10, 'Produce', 'Category', (SELECT node_id
                                      FROM product_nodes
                                     WHERE node_name = 'All Products'));

-- Produce Category Children Leaf-Level Nodes
INSERT INTO product_nodes (node_natural_key, node_name, level_name, parent_node_id)
VALUES (101, 'Spinach', 'UPC', (SELECT node_id
                                 FROM product_nodes
                                WHERE node_name = 'Produce'));

INSERT INTO product_nodes (node_natural_key, node_name, level_name, parent_node_id)
VALUES (102, 'Tomatoes', 'UPC', (SELECT node_id
                                   FROM product_nodes
                                  WHERE node_name = 'Produce'));

-- Candy Category Level Node
INSERT INTO product_nodes (node_natural_key, node_name, level_name, parent_node_id)
VALUES (20, 'Candy', 'Category', (SELECT node_id
                                   FROM product_nodes
                                  WHERE node_name = 'All Products'));

-- Candy Category Children Leaf-Level Nodes
INSERT INTO product_nodes (node_natural_key, node_name, level_name, parent_node_id)
VALUES (201, 'Hershey Bar', 'UPC', (SELECT node_id
                                    FROM product_nodes
                                   WHERE node_name = 'Candy'));

INSERT INTO product_nodes (node_natural_key, node_name, level_name, parent_node_id)
VALUES (202, 'Nerds', 'UPC', (SELECT node_id
                              FROM product_nodes
                             WHERE node_name = 'Candy'));

-- Inspect the data
SELECT *
  FROM product_nodes AS t;

-- Create a temporary table that derives root/leaf attributes from the nodes
CREATE OR REPLACE TEMPORARY TABLE product_nodes_temp
AS
SELECT node_id
     , node_natural_key
     , node_name
     , level_name
     , parent_node_id
     , CASE WHEN parent_node_id IS NULL
               THEN TRUE
               ELSE FALSE
       END AS is_root
     , CASE WHEN node_id IN (SELECT parent_node_id
                               FROM product_nodes
                            )
               THEN FALSE
               ELSE TRUE
       END AS is_leaf
  FROM product_nodes
;

-- Inspect the data
SELECT *
  FROM product_nodes_temp AS t;


-- Recursively Build a Dimension structure from the data for reporting...
CREATE OR REPLACE TABLE product_reporting_dim
AS
WITH RECURSIVE parent_nodes (
    node_id
  , node_natural_key
  , node_name
  , level_name
  , parent_node_id
  , is_root
  , is_leaf
  , level_number
  , node_json
  , node_json_path
    )
AS (
    -- Anchor Clause
    SELECT
        node_id
      , node_natural_key
      , node_name
      , level_name
      , parent_node_id
      , is_root
      , is_leaf
      , 1 AS level_number
      , {node_id: node_id,
         node_natural_key: node_natural_key,
         node_name: node_name,
         level_name: level_name,
         parent_node_id: parent_node_id,
         is_root: is_root,
         is_leaf: is_leaf,
         level_number: 1
        } AS node_json
      , [{node_id: node_id,
         node_natural_key: node_natural_key,
         node_name: node_name,
         level_name: level_name,
         parent_node_id: parent_node_id,
         is_root: is_root,
         is_leaf: is_leaf,
         level_number: 1
        }] AS node_json_path
       FROM product_nodes_temp
      WHERE parent_node_id IS NULL
    UNION ALL
    -- Recursive Clause
    SELECT
        nodes.node_id
      , nodes.node_natural_key
      , nodes.node_name
      , nodes.level_name
      , nodes.parent_node_id
      , nodes.is_root
      , nodes.is_leaf
      , parent_nodes.level_number + 1 AS level_number
      , {node_id: nodes.node_id,
         node_natural_key: nodes.node_natural_key,
         node_name: nodes.node_name,
         level_name: nodes.level_name,
         parent_node_id: nodes.parent_node_id,
         is_root: nodes.is_root,
         is_leaf: nodes.is_leaf,
         level_number: parent_nodes.level_number + 1
        } AS node_json
       , array_append (parent_nodes.node_json_path
                     , {node_id: nodes.node_id,
                        node_natural_key: nodes.node_natural_key,
                        node_name: nodes.node_name,
                        level_name: nodes.level_name,
                        parent_node_id: nodes.parent_node_id,
                        is_root: nodes.is_root,
                        is_leaf: nodes.is_leaf,
                        level_number: parent_nodes.level_number + 1
                       }
        ) AS node_json_path
       FROM product_nodes_temp AS nodes
          JOIN
            parent_nodes
          ON nodes.parent_node_id = parent_nodes.node_id
)
SELECT node_id
     , node_natural_key
     , node_name
     , level_name
     , parent_node_id
     , is_root
     , is_leaf
     , level_number
     -- Construct a new node_json struct value to include the sort order...
     , {node_id: node_id,
        node_natural_key: node_natural_key,
        node_name: node_name,
        level_name: level_name,
        parent_node_id: parent_node_id,
        is_root: is_root,
        is_leaf: is_leaf,
        level_number: level_number,
        node_sort_order: ROW_NUMBER () OVER (ORDER BY REPLACE (node_json_path::VARCHAR, ']', '') ASC NULLS LAST)} AS node_json
     , node_json_path
     , ROW_NUMBER () OVER (ORDER BY REPLACE (node_json_path::VARCHAR, ']', '') ASC NULLS LAST) AS node_sort_order
     -- Level 1 columns
     , node_json_path[1].node_id            AS level_1_node_id
     , node_json_path[1].node_natural_key   AS level_1_node_natural_key
     , node_json_path[1].node_name          AS level_1_node_name
     , node_json_path[1].level_name         AS level_1_level_name
     -- Level 2 columns
     , node_json_path[2].node_id            AS level_2_node_id
     , node_json_path[2].node_natural_key   AS level_2_node_natural_key
     , node_json_path[2].node_name          AS level_2_node_name
     , node_json_path[2].level_name         AS level_2_level_name
     -- Level 3 columns
     , node_json_path[3].node_id            AS level_3_node_id
     , node_json_path[3].node_natural_key   AS level_3_node_natural_key
     , node_json_path[3].node_name          AS level_3_node_name
     , node_json_path[3].level_name         AS level_3_level_name
     -- If you have more than 3 levels, copy a level section and paste here - using: node_json_path[n].x (where n is the level)
  FROM parent_nodes
ORDER BY node_sort_order ASC;

SELECT * EXCLUDE (node_id, parent_node_id, level_1_node_id, level_2_node_id, level_3_node_id, node_json, node_json_path)
  FROM product_reporting_dim
ORDER BY node_sort_order ASC;

CREATE TABLE sales_facts (
  product_id    INTEGER NOT NULL
, customer_id   VARCHAR (100) NOT NULL
, date_id       DATE    NOT NULL
, unit_quantity NUMERIC NOT NULL
, sales_amount  NUMERIC NOT NULL
)
;

INSERT INTO sales_facts (product_id, customer_id, date_id, unit_quantity, sales_amount)
VALUES ((SELECT node_natural_key
           FROM product_nodes
          WHERE node_name = 'Hershey Bar')
      , 'Phil'
      , DATE '2022-01-01'
      , 1
      , 3.00
       );

INSERT INTO sales_facts (product_id, customer_id, date_id, unit_quantity, sales_amount)
VALUES ((SELECT node_natural_key
           FROM product_nodes
          WHERE node_name = 'Hershey Bar')
      , 'Lottie'
      , DATE '2022-01-02'
      , 5
      , 15.00
       );

INSERT INTO sales_facts (product_id, customer_id, date_id, unit_quantity, sales_amount)
VALUES ((SELECT node_natural_key
           FROM product_nodes
          WHERE node_name = 'Nerds')
      , 'Kalie'
      , DATE '2022-01-02'
      , 2
      , 5.00
       );

INSERT INTO sales_facts (product_id, customer_id, date_id, unit_quantity, sales_amount)
VALUES ((SELECT node_natural_key
           FROM product_nodes
          WHERE node_name = 'Tomatoes')
      , 'Phil'
      , DATE '2022-01-02'
      , 2
      , 2.00
       );

INSERT INTO sales_facts (product_id, customer_id, date_id, unit_quantity, sales_amount)
VALUES ((SELECT node_natural_key
           FROM product_nodes
          WHERE node_name = 'Spinach')
      , 'Popeye'
      , DATE '2022-01-03'
      , 10
      , 5.00
       );

INSERT INTO sales_facts (product_id, customer_id, date_id, unit_quantity, sales_amount)
VALUES ((SELECT node_natural_key
           FROM product_nodes
          WHERE node_name = 'Spinach')
      , 'Brutus'
      , DATE '2022-01-04'
      , 1
      , 0.50
       );

INSERT INTO sales_facts (product_id, customer_id, date_id, unit_quantity, sales_amount)
VALUES ((SELECT node_natural_key
           FROM product_nodes
          WHERE node_name = 'Spinach')
      , 'Lottie'
      , DATE '2022-01-04'
      , 1
      , 0.50
       );

INSERT INTO sales_facts (product_id, customer_id, date_id, unit_quantity, sales_amount)
VALUES ((SELECT node_natural_key
           FROM product_nodes
          WHERE node_name = 'Spinach')
      , 'Phil'
      , DATE '2022-01-05'
      , 2
      , 2.00
       );

-- Show the sales_facts contents (join to Product for descriptions)
SELECT product_nodes.node_name AS product_name
     , sales_facts.*
  FROM sales_facts
    JOIN
       product_nodes
    ON sales_facts.product_id = product_nodes.node_natural_key;

WITH rollup_aggregations AS (
SELECT CASE WHEN GROUPING (level_3_node_id) = 0
               THEN products.level_3_node_id
            WHEN GROUPING (level_2_node_id) = 0
               THEN products.level_2_node_id
            WHEN GROUPING (level_1_node_id) = 0
               THEN products.level_1_node_id
       END AS product_node_id
     -- Aggregates
     , SUM (facts.sales_amount)           AS sum_sales_amount
     , SUM (facts.unit_quantity)          AS sum_unit_quantity
     , COUNT (DISTINCT facts.customer_id) AS distinct_customer_count
     , COUNT (*)                          AS count_of_fact_records
  FROM sales_facts AS facts
    JOIN
       product_reporting_dim AS products
    ON facts.product_id = products.node_natural_key
-- You must add more levels here if you intend to aggregate more than 3 levels...
GROUP BY ROLLUP (products.level_1_node_id
               , products.level_2_node_id
               , products.level_3_node_id
                )
-- Throw out the "GRAND TOTAL" grouping set...
HAVING NOT GROUPING (products.level_1_node_id) = 1
)
-- Now join to the product_reporting_dim to get the sort order sequence...
SELECT LPAD ('-', (product_reporting_dim.level_number - 1) * 7, '-')
     || product_reporting_dim.level_name       AS product_level_name
     , LPAD ('-', (product_reporting_dim.level_number - 1) * 7, '-')
     || product_reporting_dim.node_name        AS product_node_name
     --
     , rollup_aggregations.sum_sales_amount
     , rollup_aggregations.sum_unit_quantity
     , rollup_aggregations.distinct_customer_count
     , rollup_aggregations.count_of_fact_records
  FROM rollup_aggregations
     JOIN
       product_reporting_dim
     ON rollup_aggregations.product_node_id = product_reporting_dim.node_id
ORDER BY product_reporting_dim.node_sort_order ASC
;

-- Recursively Build an Exploded Hierarchy structure from the data for ease of aggregation...
CREATE OR REPLACE TABLE product_aggregation_dim
AS
WITH RECURSIVE parent_nodes (
    node_id
  , node_natural_key
  , node_name
  , level_name
  , parent_node_id
  , is_root
  , is_leaf
  , level_number
  , node_sort_order
  , node_json
  , node_json_path
    )
AS (
    -- Anchor Clause
    SELECT
        node_id
      , node_natural_key
      , node_name
      , level_name
      , parent_node_id
      , is_root
      , is_leaf
      , level_number
      , node_sort_order
      , node_json
      -- We must start a new NODE_JSON array b/c each node will be represented as a root node...
      , [node_json] AS node_json_path
       FROM product_reporting_dim
       -- We do NOT filter the anchor, because we want EVERY node in the hierarchy to be a root node...
    UNION ALL
    -- Recursive Clause
    SELECT
        nodes.node_id
      , nodes.node_natural_key
      , nodes.node_name
      , nodes.level_name
      , nodes.parent_node_id
      , nodes.is_root
      , nodes.is_leaf
      , nodes.level_number
      , nodes.node_sort_order
      , nodes.node_json
      , array_append (parent_nodes.node_json_path
                    , nodes.node_json
                     ) AS node_json_path
       FROM product_reporting_dim AS nodes
          JOIN
            parent_nodes
          ON nodes.parent_node_id = parent_nodes.node_id
)
SELECT -- Ancestor columns (we take the first array element to get the anchor root)
        node_json_path[1].node_id             AS ancestor_node_id
      , node_json_path[1].node_natural_key    AS ancestor_node_natural_key
      , node_json_path[1].node_name           AS ancestor_node_name
      , node_json_path[1].level_name          AS ancestor_level_name
      , node_json_path[1].level_number        AS ancestor_level_number
      , node_json_path[1].is_root             AS ancestor_is_root
      , node_json_path[1].is_leaf             AS ancestor_is_leaf
      , node_json_path[1].node_sort_order     AS ancestor_node_sort_order
      -- Descendant columns
      , node_id                               AS descendant_node_id
      , node_natural_key                      AS descendant_node_natural_key
      , node_name                             AS descendant_node_name
      , level_name                            AS descendant_level_name
      , level_number                          AS descendant_level_number
      , is_root                               AS descendant_is_root
      , is_leaf                               AS descendant_is_leaf
      , node_sort_order                       AS descendant_node_sort_order
      --
      , (level_number - node_json_path[1].level_number) AS net_level
  FROM parent_nodes
ORDER BY node_sort_order ASC;

SELECT *
 FROM product_aggregation_dim
ORDER BY ancestor_node_sort_order    ASC
       , descendant_node_sort_order  ASC
LIMIT 100;

-- Now perform easy hierarchical aggregations with the exploded hierarchy table
SELECT -- Use LPAD here to indent the node information based upon its depth in the hierarchy
       LPAD ('-', (products.ancestor_level_number - 1) * 7, '-')
     || products.ancestor_level_name       AS product_level_name
     , LPAD ('-', (products.ancestor_level_number - 1) * 7, '-')
     || products.ancestor_node_name        AS product_node_name
     -- Aggregates
     , SUM (facts.sales_amount)           AS sum_sales_amount
     , SUM (facts.unit_quantity)          AS sum_unit_quantity
     , COUNT (DISTINCT facts.customer_id) AS distinct_customer_count
     , COUNT (*)                          AS count_of_fact_records
  FROM sales_facts AS facts
    JOIN
       product_aggregation_dim AS products
    ON facts.product_id = products.descendant_node_natural_key
GROUP BY products.ancestor_node_name
       , products.ancestor_level_name
       , products.ancestor_level_number
       , products.ancestor_node_sort_order
ORDER BY products.ancestor_node_sort_order ASC
;

-- You can also aggregate with the flattened reporting dimension table, but the SQL is much more complicated
-- AND it must change depending on how many levels you are aggregating...
SELECT CASE WHEN GROUPING (level_3_node_name) = 0
               THEN '              ' || products.level_3_level_name
            WHEN GROUPING (level_2_node_name) = 0
               THEN '       ' || products.level_2_level_name
            WHEN GROUPING (level_1_node_name) = 0
               THEN products.level_1_level_name
       END AS product_level_name
     , CASE WHEN GROUPING (level_3_node_name) = 0
               THEN '              ' || products.level_3_node_name
            WHEN GROUPING (level_2_node_name) = 0
               THEN '       ' || products.level_2_node_name
            WHEN GROUPING (level_1_node_name) = 0
               THEN products.level_1_node_name
       END AS product_node_name
     -- Aggregates
     , SUM (facts.sales_amount)           AS sum_sales_amount
     , SUM (facts.unit_quantity)          AS sum_unit_quantity
     , COUNT (DISTINCT facts.customer_id) AS distinct_customer_count
     , COUNT (*)                          AS count_of_fact_records
  FROM sales_facts AS facts
    JOIN
       product_reporting_dim AS products
    ON facts.product_id = products.node_natural_key
GROUP BY ROLLUP ((products.level_1_level_name
                , products.level_1_node_name
                 )
               , (products.level_2_level_name
                , products.level_2_node_name
                 )
               , (products.level_3_level_name
                , products.level_3_node_name
                 )
                )
-- Throw out the "GRAND TOTAL" grouping set...
HAVING NOT GROUPING (products.level_1_node_name) = 1
;
