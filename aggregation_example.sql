CREATE OR REPLACE TABLE product_nodes
(
    node_id                VARCHAR(36) DEFAULT uuid()
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

-- Top Node
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

-- Recursively Build a Dimension structure from the data for reporting...
CREATE OR REPLACE TABLE product_reporting_dim
AS
WITH RECURSIVE parent_nodes (
    node_id
  , node_natural_key
  , node_name
  , level_name
  , parent_node_id
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
      , 1 AS level_number
      , {node_id: node_id,
         node_natural_key: node_natural_key,
         node_name: node_name,
         level_name: level_name,
         parent_node_id: parent_node_id,
         level_number: 1
        } AS node_json
      , [{node_id: node_id,
         node_natural_key: node_natural_key,
         node_name: node_name,
         level_name: level_name,
         parent_node_id: parent_node_id,
         level_number: 1
        }] AS node_json_path
       FROM product_nodes
      WHERE parent_node_id IS NULL
    UNION ALL
    -- Recursive Clause
    SELECT
        nodes.node_id
      , nodes.node_natural_key
      , nodes.node_name
      , nodes.level_name
      , nodes.parent_node_id
      , parent_nodes.level_number + 1 AS level_number
      , {node_id: nodes.node_id,
         node_natural_key: nodes.node_natural_key,
         node_name: nodes.node_name,
         level_name: nodes.level_name,
         parent_node_id: nodes.parent_node_id,
         level_number: parent_nodes.level_number + 1
        } AS node_json
       , array_append (parent_nodes.node_json_path
      , {node_id: nodes.node_id
      , node_natural_key: nodes.node_natural_key
      , node_name: nodes.node_name
      , level_name: nodes.level_name
      , parent_node_id: nodes.parent_node_id
      , level_number: parent_nodes.level_number + 1
        }
        ) AS node_json_path
       FROM product_nodes AS nodes
          JOIN
            parent_nodes
          ON nodes.parent_node_id = parent_nodes.node_id
)
SELECT node_id
     , node_natural_key
     , node_name
     , level_name
     , parent_node_id
     , level_number
     --
     , CASE WHEN level_number >= COALESCE (LEAD (level_number) OVER (ORDER BY REPLACE (node_json_path::VARCHAR, ']', '') ASC NULLS LAST), level_number)
               THEN TRUE
               ELSE FALSE
       END AS is_leaf
     , ROW_NUMBER () OVER (ORDER BY REPLACE (node_json_path::VARCHAR, ']', '') ASC NULLS LAST) AS node_sort_order
     -- Level 1 columns
     , node_json_path[1].node_id            AS level_1_node_id
     , node_json_path[1].node_natural_key   AS level_1_node_natural_key
     , node_json_path[1].node_name          AS level_1_node_name
     , node_json_path[1].level_name         AS level_1_level_name
     -- Level 1 columns
     , node_json_path[2].node_id            AS level_2_node_id
     , node_json_path[2].node_natural_key   AS level_2_node_natural_key
     , node_json_path[2].node_name          AS level_2_node_name
     , node_json_path[2].level_name         AS level_2_level_name
     -- Level 1 columns
     , node_json_path[3].node_id            AS level_3_node_id
     , node_json_path[3].node_natural_key   AS level_3_node_natural_key
     , node_json_path[3].node_name          AS level_3_node_name
     , node_json_path[3].level_name         AS level_3_level_name
  FROM parent_nodes
ORDER BY node_sort_order ASC;

SELECT *
  FROM product_reporting_dim
ORDER BY node_sort_order ASC;


-- Recursively Build an Exploded Hierarchy structure from the data for ease of aggregation...
CREATE OR REPLACE TABLE product_aggregation_dim
AS
WITH RECURSIVE parent_nodes (
    node_id
  , node_natural_key
  , node_name
  , level_name
  , parent_node_id
  , level_number
  , is_leaf
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
      , level_number
      , is_leaf
      , node_sort_order
      , {node_id: node_id,
         node_natural_key: node_natural_key,
         node_name: node_name,
         level_name: level_name,
         parent_node_id: parent_node_id,
         level_number: level_number,
         is_leaf: is_leaf,
         node_sort_order: node_sort_order
        } AS node_json
      , [{node_id: node_id,
         node_natural_key: node_natural_key,
         node_name: node_name,
         level_name: level_name,
         parent_node_id: parent_node_id,
         level_number: level_number,
         is_leaf: is_leaf,
         node_sort_order: node_sort_order
        }] AS node_json_path
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
      , nodes.level_number
      , nodes.is_leaf
      , nodes.node_sort_order
      , {node_id: nodes.node_id,
         node_natural_key: nodes.node_natural_key,
         node_name: nodes.node_name,
         level_name: nodes.level_name,
         parent_node_id: nodes.parent_node_id,
         level_number: nodes.level_number,
         is_leaf: nodes.is_leaf,
         node_sort_order: nodes.node_sort_order
        } AS node_json
       , array_append (parent_nodes.node_json_path
      , {node_id: nodes.node_id
      , node_natural_key: nodes.node_natural_key
      , node_name: nodes.node_name
      , level_name: nodes.level_name
      , parent_node_id: nodes.parent_node_id
      , level_number: nodes.level_number
      , is_leaf: nodes.is_leaf
      , node_sort_order: nodes.node_sort_order
        }
        ) AS node_json_path
       FROM product_reporting_dim AS nodes
          JOIN
            parent_nodes
          ON nodes.parent_node_id = parent_nodes.node_id
)
SELECT -- Ancestor columns
        node_json_path[1].node_id             AS ancestor_node_id
      , node_json_path[1].node_natural_key    AS ancestor_node_natural_key
      , node_json_path[1].node_name           AS ancestor_node_name
      , node_json_path[1].level_name          AS ancestor_level_name
      , node_json_path[1].level_number        AS ancestor_level_number
      , node_json_path[1].is_leaf             AS ancestor_is_leaf
      , node_json_path[1].node_sort_order     AS ancestor_node_sort_order
      --
      , node_id                               AS descendant_node_id
      , node_natural_key                      AS descendant_node_natural_key
      , node_name                             AS descendant_node_name
      , level_name                            AS descendant_level_name
      , level_number                          AS descendant_level_number
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

-- Now Create a Fact table
CREATE OR REPLACE TABLE sales_facts (
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

-- Now perform hierarchical aggregations
SELECT LPAD (' ', (products.ancestor_level_number - 1) * 7, ' ')
     || products.ancestor_level_name       AS product_level_name
     , LPAD (' ', (products.ancestor_level_number - 1) * 7, ' ')
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
GROUP BY products.ancestor_node_id
       , products.ancestor_node_natural_key
       , products.ancestor_node_name
       , products.ancestor_level_name
       , products.ancestor_level_number
       , products.ancestor_is_leaf
       , products.ancestor_node_sort_order
ORDER BY products.ancestor_node_sort_order ASC
;
